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

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/dispatch"
	"github.com/mongodb/mongo-go-driver/core/options"
	"github.com/mongodb/mongo-go-driver/core/readconcern"
	"github.com/mongodb/mongo-go-driver/core/readpref"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"

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

func newCollection(db *Database, name string) *Collection {
	coll := &Collection{
		client:         db.client,
		db:             db,
		name:           name,
		readPreference: db.readPreference,
		readConcern:    db.readConcern,
		writeConcern:   db.writeConcern,
		readSelector:   db.readSelector,
		writeSelector:  db.writeSelector,
	}

	return coll
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
	opts ...options.InsertOneOptioner) (*InsertOneResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo/(*Collection).InsertOne")
	defer span.End()

	span.Annotatef(nil, "Starting TransformDocument")
	doc, err := TransformDocument(document)
	span.Annotatef(nil, "Finished TransformDocument")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	span.Annotatef(nil, "Starting EnsureID", nil)
	insertedID, err := ensureID(doc)
	span.Annotatef(nil, "Finished EnsureID", nil)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	newOptions := make([]options.InsertOptioner, 0, len(opts))
	for _, opt := range opts {
		newOptions = append(newOptions, opt)
	}

	oldns := coll.namespace()
	cmd := command.Insert{
		NS:   command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs: []*bson.Document{doc},
		Opts: newOptions,
	}

	res, err := dispatch.Insert(ctx, cmd, coll.client.topology, coll.writeSelector, coll.writeConcern)
	rr, err := processWriteError(res.WriteConcernError, res.WriteErrors, err)
	if err != nil {
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
	opts ...options.InsertManyOptioner) (*InsertManyResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).InsertMany")
	defer span.End()

	result := make([]interface{}, len(documents))
	docs := make([]*bson.Document, len(documents))

	for i, doc := range documents {
		bdoc, err := TransformDocument(doc)
		if err != nil {
			span.Annotatef([]trace.Attribute{
				trace.Int64Attribute("i", int64(i)),
			}, "TransformDocument error")
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}
		insertedID, err := ensureID(bdoc)
		if err != nil {
			span.Annotatef([]trace.Attribute{
				trace.Int64Attribute("i", int64(i)),
			}, "ensureID error")
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}

		docs[i] = bdoc
		result[i] = insertedID
	}

	newOptions := make([]options.InsertOptioner, 0, len(opts))
	for _, opt := range opts {
		newOptions = append(newOptions, opt)
	}

	oldns := coll.namespace()
	cmd := command.Insert{
		NS:   command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs: docs,
		Opts: newOptions,
	}

	res, err := dispatch.Insert(ctx, cmd, coll.client.topology, coll.writeSelector, coll.writeConcern)
	switch err {
	case nil:
	case dispatch.ErrUnacknowledgedWrite:
		span.SetStatus(trace.Status{
			Code:    int32(trace.StatusCodeDataLoss),
			Message: "Unacknowleged write",
		})
		// TODO:(@odeke-em) Add stats for unacknowledgedWrites
		return &InsertManyResult{InsertedIDs: result}, ErrUnacknowledgedWrite

	default:
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if len(res.WriteErrors) > 0 || res.WriteConcernError != nil {
		err = BulkWriteError{
			WriteErrors:       writeErrorsFromResult(res.WriteErrors),
			WriteConcernError: convertWriteConcernError(res.WriteConcernError),
		}
	}
	if err != nil {
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
	opts ...options.DeleteOptioner) (*DeleteResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).DeleteOne")
	defer span.End()

	f, err := TransformDocument(filter)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}
	deleteDocs := []*bson.Document{
		bson.NewDocument(
			bson.EC.SubDocument("q", f),
			bson.EC.Int32("limit", 1)),
	}

	oldns := coll.namespace()
	cmd := command.Delete{
		NS:      command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Deletes: deleteDocs,
		Opts:    opts,
	}

	res, err := dispatch.Delete(ctx, cmd, coll.client.topology, coll.writeSelector, coll.writeConcern)
	rr, err := processWriteError(res.WriteConcernError, res.WriteErrors, err)
	if err != nil {
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
	opts ...options.DeleteOptioner) (*DeleteResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).DeleteMany")
	defer span.End()

	f, err := TransformDocument(filter)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}
	deleteDocs := []*bson.Document{bson.NewDocument(bson.EC.SubDocument("q", f), bson.EC.Int32("limit", 0))}

	oldns := coll.namespace()
	cmd := command.Delete{
		NS:      command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Deletes: deleteDocs,
		Opts:    opts,
	}

	res, err := dispatch.Delete(ctx, cmd, coll.client.topology, coll.writeSelector, coll.writeConcern)
	rr, err := processWriteError(res.WriteConcernError, res.WriteErrors, err)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	if rr&rrMany == 0 {
		return nil, err
	}
	return &DeleteResult{DeletedCount: int64(res.N)}, err
}

func (coll *Collection) updateOrReplaceOne(ctx context.Context, filter,
	update *bson.Document, opts ...options.UpdateOptioner) (*UpdateResult, error) {

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
		NS:   command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs: updateDocs,
		Opts: opts,
	}

	r, err := dispatch.Update(ctx, cmd, coll.client.topology, coll.writeSelector, coll.writeConcern)
	if err != nil && err != dispatch.ErrUnacknowledgedWrite {
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
	if err != nil {
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
	options ...options.UpdateOptioner) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).UpdateOne")
	defer span.End()

	f, err := TransformDocument(filter)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	u, err := TransformDocument(update)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if err := ensureDollarKey(u); err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInvalidArgument), Message: err.Error()})
		return nil, err
	}

	ures, err := coll.updateOrReplaceOne(ctx, f, u, options...)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return ures, err
}

// UpdateMany updates multiple documents in the collection. A user can supply
// a custom context to this method, or nil to default to context.Background().
//
// This method uses TransformDocument to turn the filter and update parameter
// into a *bson.Document. See TransformDocument for the list of valid types for
// filter and update.
func (coll *Collection) UpdateMany(ctx context.Context, filter interface{}, update interface{},
	opts ...options.UpdateOptioner) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).UpdateMany")
	defer span.End()

	f, err := TransformDocument(filter)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	u, err := TransformDocument(update)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if err = ensureDollarKey(u); err != nil {
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

	oldns := coll.namespace()
	cmd := command.Update{
		NS:   command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs: updateDocs,
		Opts: opts,
	}

	r, err := dispatch.Update(ctx, cmd, coll.client.topology, coll.writeSelector, coll.writeConcern)
	if err != nil && err != dispatch.ErrUnacknowledgedWrite {
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
	if err != nil {
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
	replacement interface{}, opts ...options.ReplaceOptioner) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).ReplaceOne")
	defer span.End()

	f, err := TransformDocument(filter)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	r, err := TransformDocument(replacement)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if elem, ok := r.ElementAtOK(0); ok && strings.HasPrefix(elem.Key(), "$") {
		span.SetStatus(trace.Status{
			Code:    int32(trace.StatusCodeInvalidArgument),
			Message: "Cannot contain keys beginning with '$'",
		})
		return nil, errors.New("replacement document cannot contains keys beginning with '$")
	}

	updateOptions := make([]options.UpdateOptioner, 0, len(opts))
	for _, opt := range opts {
		updateOptions = append(updateOptions, opt)
	}

	ures, err := coll.updateOrReplaceOne(ctx, f, r, updateOptions...)
	if err != nil {
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
	opts ...options.AggregateOptioner) (Cursor, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Aggregate")
	defer span.End()

	pipelineArr, err := transformAggregatePipeline(pipeline)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	oldns := coll.namespace()
	cmd := command.Aggregate{
		NS:       command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Pipeline: pipelineArr,
		Opts:     opts,
		ReadPref: coll.readPreference,
	}
	cur, err := dispatch.Aggregate(ctx, cmd, coll.client.topology, coll.readSelector, coll.writeSelector, coll.writeConcern)
	if err != nil {
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
	opts ...options.CountOptioner) (int64, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Count")
	defer span.End()

	f, err := TransformDocument(filter)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return 0, err
	}

	oldns := coll.namespace()
	cmd := command.Count{
		NS:       command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Query:    f,
		Opts:     opts,
		ReadPref: coll.readPreference,
	}
	count, err := dispatch.Count(ctx, cmd, coll.client.topology, coll.readSelector, coll.readConcern)
	if err != nil {
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
	opts ...options.DistinctOptioner) ([]interface{}, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Distinct")
	defer span.End()

	var f *bson.Document
	var err error
	if filter != nil {
		span.Annotatef(nil, "Invoking TransformDocument with filter")
		f, err = TransformDocument(filter)
		span.Annotatef(nil, "Finished TransformDocument with filter")
		if err != nil {
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}
	}

	oldns := coll.namespace()
	cmd := command.Distinct{
		NS:       command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Field:    fieldName,
		Query:    f,
		Opts:     opts,
		ReadPref: coll.readPreference,
	}
	res, err := dispatch.Distinct(ctx, cmd, coll.client.topology, coll.readSelector, coll.readConcern)
	if err != nil {
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
	opts ...options.FindOptioner) (Cursor, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Find")
	defer span.End()

	var f *bson.Document
	var err error
	if filter != nil {
		span.Annotatef(nil, "Invoking TransformDocument with filter")
		f, err = TransformDocument(filter)
		span.Annotatef(nil, "Finished TransformDocument with filter")
		if err != nil {
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}
	}

	oldns := coll.namespace()
	cmd := command.Find{
		NS:       command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Filter:   f,
		Opts:     opts,
		ReadPref: coll.readPreference,
	}
	cur, err := dispatch.Find(ctx, cmd, coll.client.topology, coll.readSelector, coll.readConcern)
	if err != nil {
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
	opts ...options.FindOneOptioner) *DocumentResult {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).FindOne")
	defer span.End()

	findOpts := make([]options.FindOptioner, 0, len(opts))
	for _, opt := range opts {
		findOpts = append(findOpts, opt.(options.FindOptioner))
	}

	findOpts = append(findOpts, Opt.Limit(1))

	var f *bson.Document
	var err error
	if filter != nil {
		span.Annotatef(nil, "Invoking TransformDocument with filter")
		f, err = TransformDocument(filter)
		span.Annotatef(nil, "Finished TransformDocument with filter")
		if err != nil {
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return &DocumentResult{err: err}
		}
	}

	oldns := coll.namespace()
	cmd := command.Find{
		NS:       command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Filter:   f,
		Opts:     findOpts,
		ReadPref: coll.readPreference,
	}
	cursor, err := dispatch.Find(ctx, cmd, coll.client.topology, coll.readSelector, coll.readConcern)
	if err != nil {
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
	opts ...options.FindOneAndDeleteOptioner) *DocumentResult {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).FindOneAndDelete")
	defer span.End()

	var f *bson.Document
	var err error
	if filter != nil {
		span.Annotatef(nil, "Invoking TransformDocument with filter")
		f, err = TransformDocument(filter)
		span.Annotatef(nil, "Finished TransformDocument with filter")
		if err != nil {
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return &DocumentResult{err: err}
		}
	}

	oldns := coll.namespace()
	cmd := command.FindOneAndDelete{
		NS:    command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Query: f,
		Opts:  opts,
	}
	res, err := dispatch.FindOneAndDelete(ctx, cmd, coll.client.topology, coll.writeSelector, coll.writeConcern)
	if err != nil {
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
	replacement interface{}, opts ...options.FindOneAndReplaceOptioner) *DocumentResult {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).FindOneAndReplace")
	defer span.End()

	span.Annotatef(nil, "Invoking TransformDocument with filter")
	f, err := TransformDocument(filter)
	span.Annotatef(nil, "Finished TransformDocument with filter")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	span.Annotatef(nil, "Invoking TransformDocument with replacement")
	r, err := TransformDocument(replacement)
	span.Annotatef(nil, "Finished TransformDocument with replacement")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	if elem, ok := r.ElementAtOK(0); ok && strings.HasPrefix(elem.Key(), "$") {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInvalidArgument), Message: "Cannot contain keys beginning with '$'"})
		return &DocumentResult{err: errors.New("replacement document cannot contains keys beginning with '$")}
	}

	oldns := coll.namespace()
	cmd := command.FindOneAndReplace{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Query:       f,
		Replacement: r,
		Opts:        opts,
	}
	res, err := dispatch.FindOneAndReplace(ctx, cmd, coll.client.topology, coll.writeSelector, coll.writeConcern)
	if err != nil {
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
	update interface{}, opts ...options.FindOneAndUpdateOptioner) *DocumentResult {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).FindOneAndUpdate")
	defer span.End()

	span.Annotatef(nil, "Invoking TransformDocument with filter")
	f, err := TransformDocument(filter)
	span.Annotatef(nil, "Finished TransformDocument with filter")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	span.Annotatef(nil, "Invoking TransformDocument with update")
	u, err := TransformDocument(update)
	span.Annotatef(nil, "Finished TransformDocument with update")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	if elem, ok := u.ElementAtOK(0); !ok || !strings.HasPrefix(elem.Key(), "$") {
		return &DocumentResult{err: errors.New("update document must contain key beginning with '$")}
	}

	oldns := coll.namespace()
	cmd := command.FindOneAndUpdate{
		NS:     command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Query:  f,
		Update: u,
		Opts:   opts,
	}
	res, err := dispatch.FindOneAndUpdate(ctx, cmd, coll.client.topology, coll.writeSelector, coll.writeConcern)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return &DocumentResult{err: err}
	}

	return &DocumentResult{rdr: res.Value}
}

// Watch returns a change stream cursor used to receive notifications of changes to the collection.
// This method is preferred to running a raw aggregation with a $changeStream stage because it
// supports resumability in the case of some errors.
func (coll *Collection) Watch(ctx context.Context, pipeline interface{},
	opts ...options.ChangeStreamOptioner) (Cursor, error) {
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection)")
	defer span.End()

	cur, err := newChangeStream(ctx, coll, pipeline, opts...)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return cur, err
}

// Indexes returns the index view for this collection.
func (coll *Collection) Indexes() IndexView {
	return IndexView{coll: coll}
}

// Drop drops this collection from database.
func (coll *Collection) Drop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Collection).Drop")
	defer span.End()

	cmd := command.DropCollection{
		DB:         coll.db.name,
		Collection: coll.name,
	}
	_, err := dispatch.DropCollection(ctx, cmd, coll.client.topology, coll.writeSelector)
	if err != nil && !command.IsNotFound(err) {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return err
	}
	return nil
}
