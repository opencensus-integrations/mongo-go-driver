// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/dispatch"
	"github.com/mongodb/mongo-go-driver/core/option"
	"github.com/mongodb/mongo-go-driver/core/readconcern"
	"github.com/mongodb/mongo-go-driver/core/readpref"
	"github.com/mongodb/mongo-go-driver/core/session"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
	"github.com/mongodb/mongo-go-driver/mongo/aggregateopt"
	"github.com/mongodb/mongo-go-driver/mongo/changestreamopt"
	"github.com/mongodb/mongo-go-driver/mongo/collectionopt"
	"github.com/mongodb/mongo-go-driver/mongo/countopt"
	"github.com/mongodb/mongo-go-driver/mongo/deleteopt"
	"github.com/mongodb/mongo-go-driver/mongo/distinctopt"
	"github.com/mongodb/mongo-go-driver/mongo/dropcollopt"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"github.com/mongodb/mongo-go-driver/mongo/insertopt"
	"github.com/mongodb/mongo-go-driver/mongo/replaceopt"
	"github.com/mongodb/mongo-go-driver/mongo/updateopt"

	"github.com/mongodb/mongo-go-driver/internal/observability"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// Collection performs operations on a given collection.
type Collection struct {
	client         *Client
	db             *Database
	name           string
	readConcern    *readconcern.ReadConcern
	writeConcern   *writeconcern.WriteConcern
	readPreference *readpref.ReadPref
	readSelector   description.ServerSelector
	writeSelector  description.ServerSelector
}

func newCollection(db *Database, name string, opts ...collectionopt.Option) *Collection {
	collOpt, err := collectionopt.BundleCollection(opts...).Unbundle()
	if err != nil {
		return nil
	}

	rc := db.readConcern
	if collOpt.ReadConcern != nil {
		rc = collOpt.ReadConcern
	}

	wc := db.writeConcern
	if collOpt.WriteConcern != nil {
		wc = collOpt.WriteConcern
	}

	rp := db.readPreference
	if collOpt.ReadPreference != nil {
		rp = collOpt.ReadPreference
	}

	coll := &Collection{
		client:         db.client,
		db:             db,
		name:           name,
		readPreference: rp,
		readConcern:    rc,
		writeConcern:   wc,
		readSelector:   db.readSelector,
		writeSelector:  db.writeSelector,
	}

	return coll
}

func (coll *Collection) copy() *Collection {
	return &Collection{
		client:         coll.client,
		db:             coll.db,
		name:           coll.name,
		readConcern:    coll.readConcern,
		writeConcern:   coll.writeConcern,
		readPreference: coll.readPreference,
		readSelector:   coll.readSelector,
		writeSelector:  coll.writeSelector,
	}
}

// Clone creates a copy of this collection with updated options, if any are given.
func (coll *Collection) Clone(opts ...collectionopt.Option) (*Collection, error) {
	copyColl := coll.copy()
	optsColl, err := collectionopt.BundleCollection(opts...).Unbundle()
	if err != nil {
		return nil, err
	}

	if optsColl.ReadConcern != nil {
		copyColl.readConcern = optsColl.ReadConcern
	}

	if optsColl.WriteConcern != nil {
		copyColl.writeConcern = optsColl.WriteConcern
	}

	if optsColl.ReadPreference != nil {
		copyColl.readPreference = optsColl.ReadPreference
	}

	return copyColl, nil
}

// Name provides access to the name of the collection.
func (coll *Collection) Name() string {
	return coll.name
}

// namespace returns the namespace of the collection.
func (coll *Collection) namespace() command.Namespace {
	return command.NewNamespace(coll.db.name, coll.name)
}

// InsertOne inserts a single document into the collection. A user can supply
// a custom context to this method, or nil to default to context.Background().
//
// This method uses TransformDocument to turn the document parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// document.
//
// TODO(skriptble): Determine if we should unwrap the value for the
// InsertOneResult or just return the bson.Element or a bson.Value.
func (coll *Collection) InsertOne(ctx context.Context, document interface{},
	opts ...insertopt.One) (*InsertOneResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "insert_one"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).InsertOne")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	span.Annotate(nil, "Starting TransformDocument")
	doc, err := TransformDocument(document)
	span.Annotate(nil, "Finished TransformDocument")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	span.Annotate(nil, "Starting EnsureID")
	insertedID, err := ensureID(doc)
	span.Annotate(nil, "Finished EnsureID")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "ensure_id"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	// convert options into []option.InsertOptioner and dedup
	oneOpts, sess, err := insertopt.BundleOne(opts...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	oldns := coll.namespace()
	cmd := command.Insert{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs:         []*bson.Document{doc},
		Opts:         oneOpts,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := dispatch.Insert(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)

	rr, err := processWriteError(res.WriteConcernError, res.WriteErrors, err)

	if err == nil {
		stats.Record(ctx, observability.MInsertions.M(1))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "dispatch_insert"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}

	if rr&rrOne == 0 {
		return nil, err
	}

	return &InsertOneResult{InsertedID: insertedID}, err
}

// InsertMany inserts the provided documents. A user can supply a custom context to this
// method.
//
// Currently, batching is not implemented for this operation. Because of this, extremely large
// sets of documents will not fit into a single BSON document to be sent to the server, so the
// operation will fail.
//
// This method uses TransformDocument to turn the documents parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// documents.
func (coll *Collection) InsertMany(ctx context.Context, documents []interface{},
	opts ...insertopt.Many) (*InsertManyResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "insert_many"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).InsertMany")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	result := make([]interface{}, len(documents))
	docs := make([]*bson.Document, len(documents))

	for i, doc := range documents {
		bdoc, err := TransformDocument(doc)
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.Annotatef([]trace.Attribute{
				trace.Int64Attribute("i", int64(i)),
			}, "TransformDocument error")
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}
		insertedID, err := ensureID(bdoc)
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "ensure_doc"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.Annotatef([]trace.Attribute{
				trace.Int64Attribute("i", int64(i)),
			}, "ensureID error")
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}

		docs[i] = bdoc
		result[i] = insertedID
	}

	// convert options into []option.InsertOptioner and dedup
	manyOpts, sess, err := insertopt.BundleMany(opts...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	oldns := coll.namespace()
	cmd := command.Insert{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs:         docs,
		Opts:         manyOpts,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := dispatch.Insert(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)

	switch err {
	case nil:
	case command.ErrUnacknowledgedWrite:
		return &InsertManyResult{InsertedIDs: result}, ErrUnacknowledgedWrite

	default:
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "dispatch_insert"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if len(res.WriteErrors) > 0 || res.WriteConcernError != nil {
		err = BulkWriteError{
			WriteErrors:       writeErrorsFromResult(res.WriteErrors),
			WriteConcernError: convertWriteConcernError(res.WriteConcernError),
		}
	}

	if err == nil {
		stats.Record(ctx, observability.MInsertions.M(1))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "dispatch_insert"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}

	return &InsertManyResult{InsertedIDs: result}, err
}

// DeleteOne deletes a single document from the collection. A user can supply
// a custom context to this method, or nil to default to context.Background().
//
// This method uses TransformDocument to turn the filter parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// filter.
func (coll *Collection) DeleteOne(ctx context.Context, filter interface{},
	opts ...deleteopt.Delete) (*DeleteResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "delete_one"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).DeleteOne")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	f, err := TransformDocument(filter)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}
	deleteDocs := []*bson.Document{
		bson.NewDocument(
			bson.EC.SubDocument("q", f),
			bson.EC.Int32("limit", 1)),
	}

	deleteOpts, sess, err := deleteopt.BundleDelete(opts...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	oldns := coll.namespace()
	cmd := command.Delete{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Deletes:      deleteDocs,
		Opts:         deleteOpts,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := dispatch.Delete(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)

	rr, err := processWriteError(res.WriteConcernError, res.WriteErrors, err)
	if err == nil {
		stats.Record(ctx, observability.MDeletions.M(1))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "dispatch_delete"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	if rr&rrOne == 0 {
		return nil, err
	}
	return &DeleteResult{DeletedCount: int64(res.N)}, err
}

// DeleteMany deletes multiple documents from the collection. A user can
// supply a custom context to this method, or nil to default to
// context.Background().
//
// This method uses TransformDocument to turn the filter parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// filter.
func (coll *Collection) DeleteMany(ctx context.Context, filter interface{},
	opts ...deleteopt.Delete) (*DeleteResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "delete_many"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).DeleteMany")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	f, err := TransformDocument(filter)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Insert(observability.KeyPart, "transform_document"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}
	deleteDocs := []*bson.Document{bson.NewDocument(bson.EC.SubDocument("q", f), bson.EC.Int32("limit", 0))}

	deleteOpts, sess, err := deleteopt.BundleDelete(opts...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	oldns := coll.namespace()
	cmd := command.Delete{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Deletes:      deleteDocs,
		Opts:         deleteOpts,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := dispatch.Delete(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)

	rr, err := processWriteError(res.WriteConcernError, res.WriteErrors, err)
	if err == nil {
		stats.Record(ctx, observability.MDeletions.M(1))
	} else {
		ctx, _ = tag.New(ctx, tag.Insert(observability.KeyPart, "dispatch_delete"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}

	if rr&rrMany == 0 {
		return nil, err
	}
	return &DeleteResult{DeletedCount: int64(res.N)}, err
}

func (coll *Collection) updateOrReplaceOne(ctx context.Context, filter,
	update *bson.Document, sess *session.Client, opts ...option.UpdateOptioner) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).updateOrReplaceOne")
	defer span.End()

	updateDocs := []*bson.Document{
		bson.NewDocument(
			bson.EC.SubDocument("q", filter),
			bson.EC.SubDocument("u", update),
			bson.EC.Boolean("multi", false),
		),
	}

	oldns := coll.namespace()
	cmd := command.Update{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs:         updateDocs,
		Opts:         opts,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	r, err := dispatch.Update(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil && err != command.ErrUnacknowledgedWrite {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "dispatch_update"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	res := &UpdateResult{
		MatchedCount:  r.MatchedCount,
		ModifiedCount: r.ModifiedCount,
	}
	if len(r.Upserted) > 0 {
		res.UpsertedID = r.Upserted[0].ID
		res.MatchedCount--
	}

	rr, err := processWriteError(r.WriteConcernError, r.WriteErrors, err)
	if err == nil {
		stats.Record(ctx, observability.MUpdates.M(1), observability.MReplaces.M(1))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "process_write_error"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	if rr&rrOne == 0 {
		return nil, err
	}
	return res, err
}

// UpdateOne updates a single document in the collection. A user can supply a
// custom context to this method, or nil to default to context.Background().
//
// This method uses TransformDocument to turn the filter and update parameter
// into a *bson.Document. See TransformDocument for the list of valid types for
// filter and update.
func (coll *Collection) UpdateOne(ctx context.Context, filter interface{}, update interface{},
	options ...updateopt.Update) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "update_one"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).UpdateOne")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	f, err := TransformDocument(filter)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	u, err := TransformDocument(update)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_update"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if err := ensureDollarKey(u); err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "ensure_dollar_key"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInvalidArgument), Message: err.Error()})
		return nil, err
	}

	updOpts, sess, err := updateopt.BundleUpdate(options...).Unbundle(true)
	if err != nil {
		// updateOrReplaceOne already sets error metrics
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	return coll.updateOrReplaceOne(ctx, f, u, sess, updOpts...)
}

// UpdateMany updates multiple documents in the collection. A user can supply
// a custom context to this method, or nil to default to context.Background().
//
// This method uses TransformDocument to turn the filter and update parameter
// into a *bson.Document. See TransformDocument for the list of valid types for
// filter and update.
func (coll *Collection) UpdateMany(ctx context.Context, filter interface{}, update interface{},
	opts ...updateopt.Update) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "update_many"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).UpdateMany")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	f, err := TransformDocument(filter)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	u, err := TransformDocument(update)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_update"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if err = ensureDollarKey(u); err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "ensure_dollar_key"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	updateDocs := []*bson.Document{
		bson.NewDocument(
			bson.EC.SubDocument("q", f),
			bson.EC.SubDocument("u", u),
			bson.EC.Boolean("multi", true),
		),
	}

	updOpts, sess, err := updateopt.BundleUpdate(opts...).Unbundle(true)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "updateopt_bundleupdate"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "client_validsession"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	oldns := coll.namespace()
	cmd := command.Update{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs:         updateDocs,
		Opts:         updOpts,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	r, err := dispatch.Update(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil && err != command.ErrUnacknowledgedWrite {
		// dispatch.Update already sets error metrics
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}
	res := &UpdateResult{
		MatchedCount:  r.MatchedCount,
		ModifiedCount: r.ModifiedCount,
	}
	// TODO(skriptble): Is this correct? Do we only return the first upserted ID for an UpdateMany?
	if len(r.Upserted) > 0 {
		res.UpsertedID = r.Upserted[0].ID
		res.MatchedCount--
	}

	rr, err := processWriteError(r.WriteConcernError, r.WriteErrors, err)
	if err == nil {
		stats.Record(ctx, observability.MUpdates.M(1))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "process_write_error"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	if rr&rrMany == 0 {
		return nil, err
	}

	return res, err
}

// ReplaceOne replaces a single document in the collection. A user can supply
// a custom context to this method, or nil to default to context.Background().
//
// This method uses TransformDocument to turn the filter and replacement
// parameter into a *bson.Document. See TransformDocument for the list of
// valid types for filter and replacement.
func (coll *Collection) ReplaceOne(ctx context.Context, filter interface{},
	replacement interface{}, opts ...replaceopt.Replace) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "replace_one"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).ReplaceOne")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	f, err := TransformDocument(filter)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	r, err := TransformDocument(replacement)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_update"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if elem, ok := r.ElementAtOK(0); ok && strings.HasPrefix(elem.Key(), "$") {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "elem_ok"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{
			Code:    int32(trace.StatusCodeInvalidArgument),
			Message: "Cannot contain keys beginning with '$'",
		})
		return nil, errors.New("replacement document cannot contains keys beginning with '$")
	}

	repOpts, sess, err := replaceopt.BundleReplace(opts...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	updateOptions := make([]option.UpdateOptioner, 0, len(opts))
	for _, opt := range repOpts {
		updateOptions = append(updateOptions, opt)
	}

	ures, err := coll.updateOrReplaceOne(ctx, f, r, sess, updateOptions...)
	if err != nil {
		// updateOrReplaceOne already sets error metrics
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return ures, err
}

// Aggregate runs an aggregation framework pipeline. A user can supply a custom context to
// this method.
//
// See https://docs.mongodb.com/manual/aggregation/.
//
// This method uses TransformDocument to turn the pipeline parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// pipeline.
func (coll *Collection) Aggregate(ctx context.Context, pipeline interface{},
	opts ...aggregateopt.Aggregate) (Cursor, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "aggregate"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Aggregate")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	pipelineArr, err := transformAggregatePipeline(pipeline)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_aggregate_pipeline"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	// convert options into []option.Optioner and dedup
	aggOpts, sess, err := aggregateopt.BundleAggregate(opts...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	oldns := coll.namespace()
	cmd := command.Aggregate{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Pipeline:     pipelineArr,
		Opts:         aggOpts,
		ReadPref:     coll.readPreference,
		WriteConcern: coll.writeConcern,
		ReadConcern:  coll.readConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	cur, err := dispatch.Aggregate(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil {
		// dispatch.Aggregate already sets error metrics
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return cur, err

}

// Count gets the number of documents matching the filter. A user can supply a
// custom context to this method, or nil to default to context.Background().
//
// This method uses TransformDocument to turn the filter parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// filter.
func (coll *Collection) Count(ctx context.Context, filter interface{},
	opts ...countopt.Count) (int64, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "count"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Count")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)))
		span.End()
	}()

	f, err := TransformDocument(filter)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return 0, err
	}

	countOpts, sess, err := countopt.BundleCount(opts...).Unbundle(true)
	if err != nil {
		return 0, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return 0, err
	}

	oldns := coll.namespace()
	cmd := command.Count{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Query:       f,
		Opts:        countOpts,
		ReadPref:    coll.readPreference,
		ReadConcern: coll.readConcern,
		Session:     sess,
		Clock:       coll.client.clock,
	}

	count, err := dispatch.Count(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil {
		// dispatch.Count already sets error metrics
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return count, err
}

// Distinct finds the distinct values for a specified field across a single
// collection. A user can supply a custom context to this method, or nil to
// default to context.Background().
//
// This method uses TransformDocument to turn the filter parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// filter.
func (coll *Collection) Distinct(ctx context.Context, fieldName string, filter interface{},
	opts ...distinctopt.Distinct) ([]interface{}, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "distinct"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Distinct")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	var f *bson.Document
	var err error
	if filter != nil {
		span.Annotatef(nil, "Invoking TransformDocument with filter")
		f, err = TransformDocument(filter)
		span.Annotatef(nil, "Finished TransformDocument with filter")
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}
	}

	distinctOpts, sess, err := distinctopt.BundleDistinct(opts...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	oldns := coll.namespace()
	cmd := command.Distinct{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Field:       fieldName,
		Query:       f,
		Opts:        distinctOpts,
		ReadPref:    coll.readPreference,
		ReadConcern: coll.readConcern,
		Session:     sess,
		Clock:       coll.client.clock,
	}

	res, err := dispatch.Distinct(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil {
		// dispatch.Distinct already sets error metrics
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	return res.Values, nil
}

// Find finds the documents matching a model. A user can supply a custom context to this
// method.
//
// This method uses TransformDocument to turn the filter parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// filter.
func (coll *Collection) Find(ctx context.Context, filter interface{},
	opts ...findopt.Find) (Cursor, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "find"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Find")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	var f *bson.Document
	var err error
	if filter != nil {
		span.Annotatef(nil, "Invoking TransformDocument with filter")
		f, err = TransformDocument(filter)
		span.Annotatef(nil, "Finished TransformDocument with filter")
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}
	}

	findOpts, sess, err := findopt.BundleFind(opts...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	oldns := coll.namespace()
	cmd := command.Find{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Filter:      f,
		Opts:        findOpts,
		ReadPref:    coll.readPreference,
		ReadConcern: coll.readConcern,
		Session:     sess,
		Clock:       coll.client.clock,
	}

	cur, err := dispatch.Find(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil {
		// dispatch.Find already sets error metrics
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return cur, err
}

// FindOne returns up to one document that matches the model. A user can
// supply a custom context to this method, or nil to default to
// context.Background().
//
// This method uses TransformDocument to turn the filter parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// filter.
func (coll *Collection) FindOne(ctx context.Context, filter interface{},
	opts ...findopt.One) *DocumentResult {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "find_one"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).FindOne")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	var f *bson.Document
	var err error
	if filter != nil {
		span.Annotatef(nil, "Invoking TransformDocument with filter")
		f, err = TransformDocument(filter)
		span.Annotatef(nil, "Finished TransformDocument with filter")
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return &DocumentResult{err: err}
		}
	}

	findOneOpts, sess, err := findopt.BundleOne(opts...).Unbundle(true)
	if err != nil {
		return &DocumentResult{err: err}
	}
	findOneOpts = append(findOneOpts, findopt.Limit(1).ConvertFindOption())

	err = coll.client.ValidSession(sess)
	if err != nil {
		return &DocumentResult{err: err}
	}

	oldns := coll.namespace()
	cmd := command.Find{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Filter:      f,
		Opts:        findOneOpts,
		ReadPref:    coll.readPreference,
		ReadConcern: coll.readConcern,
		Session:     sess,
		Clock:       coll.client.clock,
	}

	cursor, err := dispatch.Find(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil {
		// dispatch.Find already sets error metrics
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	return &DocumentResult{cur: cursor}
}

// FindOneAndDelete find a single document and deletes it, returning the
// original in result.  The document to return may be nil.
//
// A user can supply a custom context to this method, or nil to default to
// context.Background().
//
// This method uses TransformDocument to turn the filter parameter into a
// *bson.Document. See TransformDocument for the list of valid types for
// filter.
func (coll *Collection) FindOneAndDelete(ctx context.Context, filter interface{},
	opts ...findopt.DeleteOne) *DocumentResult {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "find_one_and_delete"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).FindOneAndDelete")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	var f *bson.Document
	var err error
	if filter != nil {
		span.Annotatef(nil, "Invoking TransformDocument with filter")
		f, err = TransformDocument(filter)
		span.Annotatef(nil, "Finished TransformDocument with filter")
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return &DocumentResult{err: err}
		}
	}

	findOpts, sess, err := findopt.BundleDeleteOne(opts...).Unbundle(true)
	if err != nil {
		return &DocumentResult{err: err}
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return &DocumentResult{err: err}
	}

	oldns := coll.namespace()
	cmd := command.FindOneAndDelete{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Query:        f,
		Opts:         findOpts,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := dispatch.FindOneAndDelete(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil {
		// dispatch.FindOneAndDelete already sets error metrics
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	return &DocumentResult{rdr: res.Value}
}

// FindOneAndReplace finds a single document and replaces it, returning either
// the original or the replaced document. The document to return may be nil.
//
// A user can supply a custom context to this method, or nil to default to
// context.Background().
//
// This method uses TransformDocument to turn the filter and replacement
// parameter into a *bson.Document. See TransformDocument for the list of
// valid types for filter and replacement.
func (coll *Collection) FindOneAndReplace(ctx context.Context, filter interface{},
	replacement interface{}, opts ...findopt.ReplaceOne) *DocumentResult {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "find_one_and_replace"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).FindOneAndReplace")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	span.Annotatef(nil, "Invoking TransformDocument with filter")
	f, err := TransformDocument(filter)
	span.Annotatef(nil, "Finished TransformDocument with filter")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	span.Annotatef(nil, "Invoking TransformDocument with replacement")
	r, err := TransformDocument(replacement)
	span.Annotatef(nil, "Finished TransformDocument with replacement")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_update"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	if elem, ok := r.ElementAtOK(0); ok && strings.HasPrefix(elem.Key(), "$") {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "elem_ok"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInvalidArgument), Message: "Cannot contain keys beginning with '$'"})
		return &DocumentResult{err: errors.New("replacement document cannot contains keys beginning with '$")}
	}

	findOpts, sess, err := findopt.BundleReplaceOne(opts...).Unbundle(true)

	if err != nil {
		return &DocumentResult{err: err}
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return &DocumentResult{err: err}
	}

	oldns := coll.namespace()
	cmd := command.FindOneAndReplace{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Query:        f,
		Replacement:  r,
		Opts:         findOpts,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := dispatch.FindOneAndReplace(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil {
		// dispatch.FindOneAndReplace already sets error metrics
		return &DocumentResult{err: err}
	}

	return &DocumentResult{rdr: res.Value}
}

// FindOneAndUpdate finds a single document and updates it, returning either
// the original or the updated. The document to return may be nil.
//
// A user can supply a custom context to this method, or nil to default to
// context.Background().
//
// This method uses TransformDocument to turn the filter and update parameter
// into a *bson.Document. See TransformDocument for the list of valid types for
// filter and update.
func (coll *Collection) FindOneAndUpdate(ctx context.Context, filter interface{},
	update interface{}, opts ...findopt.UpdateOne) *DocumentResult {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "findOneAndUpdate"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).FindOneAndUpdate")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	span.Annotatef(nil, "Invoking TransformDocument with filter")
	f, err := TransformDocument(filter)
	span.Annotatef(nil, "Finished TransformDocument with filter")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_filter"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	span.Annotatef(nil, "Invoking TransformDocument with update")
	u, err := TransformDocument(update)
	span.Annotatef(nil, "Finished TransformDocument with update")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "transform_document_update"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	if elem, ok := u.ElementAtOK(0); !ok || !strings.HasPrefix(elem.Key(), "$") {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "elem_ok"))
		stats.Record(ctx, observability.MErrors.M(1))
		return &DocumentResult{err: errors.New("update document must contain key beginning with '$")}
	}

	findOpts, sess, err := findopt.BundleUpdateOne(opts...).Unbundle(true)
	if err != nil {
		return &DocumentResult{err: err}
	}

	err = coll.client.ValidSession(sess)
	if err != nil {
		return &DocumentResult{err: err}
	}

	oldns := coll.namespace()
	cmd := command.FindOneAndUpdate{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Query:        f,
		Update:       u,
		Opts:         findOpts,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := dispatch.FindOneAndUpdate(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil {
		// dispatch.FindOneAndUpdate already sets error metrics.
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	return &DocumentResult{rdr: res.Value}
}

// Watch returns a change stream cursor used to receive notifications of changes to the collection.
// This method is preferred to running a raw aggregation with a $changeStream stage because it
// supports resumability in the case of some errors.
func (coll *Collection) Watch(ctx context.Context, pipeline interface{},
	opts ...changestreamopt.ChangeStream) (Cursor, error) {
	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "watch"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Watch")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	cur, err := newChangeStream(ctx, coll, pipeline, opts...)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyMethod, "new_change_stream"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return cur, err
}

// Indexes returns the index view for this collection.
func (coll *Collection) Indexes() IndexView {
	return IndexView{coll: coll}
}

// Drop drops this collection from database.
func (coll *Collection) Drop(ctx context.Context, opts ...dropcollopt.DropColl) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "drop"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Drop")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	var sess *session.Client
	for _, opt := range opts {
		if conv, ok := opt.(dropcollopt.DropCollSession); ok {
			sess = conv.ConvertDropCollSession()
		}
	}

	err := coll.client.ValidSession(sess)
	if err != nil {
		return err
	}

	cmd := command.DropCollection{
		DB:           coll.db.name,
		Collection:   coll.name,
		WriteConcern: coll.writeConcern,
		Session:      sess,
		Clock:        coll.client.clock,
	}
	_, err = dispatch.DropCollection(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil && !command.IsNotFound(err) {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "dispatch_dropcollection"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return err
	}
	return nil
}
