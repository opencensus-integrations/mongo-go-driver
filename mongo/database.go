// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
        "time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/dispatch"
	"github.com/mongodb/mongo-go-driver/core/readconcern"
	"github.com/mongodb/mongo-go-driver/core/readpref"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"

	"github.com/mongodb/mongo-go-driver/internal/observability"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// Database performs operations on a given database.
type Database struct {
	client         *Client
	name           string
	readConcern    *readconcern.ReadConcern
	writeConcern   *writeconcern.WriteConcern
	readPreference *readpref.ReadPref
	readSelector   description.ServerSelector
	writeSelector  description.ServerSelector
}

func newDatabase(client *Client, name string) *Database {
	db := &Database{
		client:         client,
		name:           name,
		readPreference: client.readPreference,
		readConcern:    client.readConcern,
		writeConcern:   client.writeConcern,
	}

	db.readSelector = description.CompositeSelector([]description.ServerSelector{
		description.ReadPrefSelector(db.readPreference),
		description.LatencySelector(db.client.localThreshold),
	})

	db.writeSelector = description.WriteSelector()

	return db
}

// Client returns the Client the database was created from.
func (db *Database) Client() *Client {
	return db.client
}

// Name returns the name of the database.
func (db *Database) Name() string {
	return db.name
}

// Collection gets a handle for a given collection in the database.
func (db *Database) Collection(name string) *Collection {
	return newCollection(db, name)
}

// RunCommand runs a command on the database. A user can supply a custom
// context to this method, or nil to default to context.Background().
func (db *Database) RunCommand(ctx context.Context, runCommand interface{}) (bson.Reader, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "db_runcommand"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Database).RunCommand")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	cmd := command.Command{DB: db.Name(), Command: runCommand}
	br, err := dispatch.Command(ctx, cmd, db.client.topology, db.writeSelector)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "dispatch_command"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return br, err
}

// Drop drops this database from mongodb.
func (db *Database) Drop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "db_drop"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/mongo.(*Database).Drop")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, observability.MRoundTripLatencyMilliseconds.M(observability.SinceInMilliseconds(startTime)), observability.MCalls.M(1))
		span.End()
	}()

	cmd := command.DropDatabase{
		DB: db.name,
	}
	_, err := dispatch.DropDatabase(ctx, cmd, db.client.topology, db.writeSelector)
	if err != nil && !command.IsNotFound(err) {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "dispatch_dropdatabase"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return err
	}
	return nil
}
