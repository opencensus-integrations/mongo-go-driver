// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"bytes"
	"context"
	"errors"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/option"
	"github.com/mongodb/mongo-go-driver/core/session"
	"github.com/mongodb/mongo-go-driver/mongo/changestreamopt"

	"go.opencensus.io/trace"
)

// ErrMissingResumeToken indicates that a change stream notification from the server did not
// contain a resume token.
var ErrMissingResumeToken = errors.New("cannot provide resume functionality when the resume token is missing")

type changeStream struct {
	pipeline    *bson.Array
	options     []option.ChangeStreamOptioner
	coll        *Collection
	cursor      Cursor
	session     *session.Client
	clock       *session.ClusterClock
	resumeToken *bson.Document
	err         error
}

const errorCodeNotMaster int32 = 10107
const errorCodeCursorNotFound int32 = 43

func newChangeStream(ctx context.Context, coll *Collection, pipeline interface{},
	opts ...changestreamopt.ChangeStream) (*changeStream, error) {

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.newChangeStream")
	defer span.End()

	span.Annotatef(nil, "Started aggregate pipeline transformation")
	pipelineArr, err := transformAggregatePipeline(pipeline)
	span.Annotatef(nil, "Finished aggregate pipeline transformation")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	csOpts, sess, err := changestreamopt.BundleChangeStream(opts...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	changeStreamOptions := bson.NewDocument()

	for _, opt := range csOpts {
		err = opt.Option(changeStreamOptions)
		if err != nil {
			return nil, err
		}
	}

	pipelineArr.Prepend(
		bson.VC.Document(
			bson.NewDocument(
				bson.EC.SubDocument("$changeStream", changeStreamOptions))))

	span.Annotatef(nil, "Starting the pipeline aggregation")
	cursor, err := coll.Aggregate(ctx, pipelineArr)
	span.Annotatef(nil, "Finished the pipeline aggregation")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	cs := &changeStream{
		pipeline: pipelineArr,
		options:  csOpts,
		coll:     coll,
		cursor:   cursor,
		session:  sess,
		clock:    coll.client.clock,
	}

	return cs, nil
}

func (cs *changeStream) ID() int64 {
	return cs.cursor.ID()
}

func (cs *changeStream) Next(ctx context.Context) bool {
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*changeStream).Next")
	defer span.End()

	span.Annotatef(nil, "Invoking next")

	if cs.cursor.Next(ctx) {
		return true
	}

	err := cs.cursor.Err()
	if err == nil {
		return false
	}

	switch t := err.(type) {
	case command.Error:
		if t.Code != errorCodeNotMaster && t.Code != errorCodeCursorNotFound {
			return false
		}
	}

	resumeToken := changestreamopt.ResumeAfter(cs.resumeToken).ConvertChangeStreamOption()
	found := false

	for i, opt := range cs.options {
		if _, ok := opt.(option.OptResumeAfter); ok {
			cs.options[i] = resumeToken
			found = true
			break
		}
	}

	if !found {
		cs.options = append(cs.options, resumeToken)
	}

	oldns := cs.coll.namespace()
	killCursors := command.KillCursors{
		NS:  command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		IDs: []int64{cs.ID()},
	}

	span.Annotatef(nil, "Selecting the server in the topology")
	ss, err := cs.coll.client.topology.SelectServer(ctx, cs.coll.readSelector)
	span.Annotatef(nil, "Finished selecting the server in the topology")
	if err != nil {
		cs.err = err
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return false
	}

	span.Annotatef(nil, "Retrieving the connection")
	conn, err := ss.Connection(ctx)
	span.Annotatef(nil, "Finished retrieving the connection")
	if err != nil {
		cs.err = err
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return false
	}
	defer conn.Close()

	_, _ = killCursors.RoundTrip(ctx, ss.Description(), conn)

	changeStreamOptions := bson.NewDocument()

	for _, opt := range cs.options {
		err = opt.Option(changeStreamOptions)
		if err != nil {
			cs.err = err
			return false
		}
	}

	cs.pipeline.Set(0, bson.VC.Document(
		bson.NewDocument(
			bson.EC.SubDocument("$changeStream", changeStreamOptions)),
	),
	)

	oldns = cs.coll.namespace()
	aggCmd := command.Aggregate{
		NS:       command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Pipeline: cs.pipeline,
		Session:  cs.session,
		Clock:    cs.coll.client.clock,
	}

	span.Annotatef(nil, "Now invoking aggregate command RoundTrip")
	cur, err := aggCmd.RoundTrip(ctx, ss.Description(), ss, conn)
	span.Annotatef(nil, "Finished invoking aggregate command RoundTrip")
	cs.cursor = cur
	cs.err = err

	if cs.err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: cs.err.Error()})
		return false
	}

	return cs.cursor.Next(ctx)
}

func (cs *changeStream) Decode(out interface{}) error {
	br, err := cs.DecodeBytes()
	if err != nil {
		return err
	}

	return bson.NewDecoder(bytes.NewReader(br)).Decode(out)
}

func (cs *changeStream) DecodeBytes() (bson.Reader, error) {
	br, err := cs.cursor.DecodeBytes()
	if err != nil {
		return nil, err
	}

	id, err := br.Lookup("_id")
	if err != nil {
		_ = cs.Close(context.Background())
		return nil, ErrMissingResumeToken
	}

	cs.resumeToken = id.Value().MutableDocument()

	return br, nil
}

func (cs *changeStream) Err() error {
	if cs.err != nil {
		return cs.err
	}

	return cs.cursor.Err()
}

func (cs *changeStream) Close(ctx context.Context) error {
	return cs.cursor.Close(ctx)
}
