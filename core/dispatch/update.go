// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package dispatch

import (
	"context"

	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/result"
	"github.com/mongodb/mongo-go-driver/core/session"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/core/uuid"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"

	"github.com/mongodb/mongo-go-driver/internal/observability"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// Update handles the full cycle dispatch and execution of an update command against the provided
// topology.
func Update(
	ctx context.Context,
	cmd command.Update,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
) (result.Update, error) {

	ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyMethod, "update"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/core/dispatch.Update")
	defer span.End()

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "connect"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return result.Update{}, err
	}

	desc := ss.Description()
	span.Annotatef(nil, "Starting ss.Connection")
	conn, err := ss.Connection(ctx)
	span.Annotatef(nil, "Finished invoking ss.Connection")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "connection"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return result.Update{}, err
	}

	if !writeconcern.AckWrite(cmd.WriteConcern) {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()

			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "write"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: "Unacknowledged writes"})

		return result.Update{}, command.ErrUnacknowledgedWrite
	}
	defer conn.Close()

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return result.Update{}, err
		}
		defer cmd.Session.EndSession()
	}

	span.Annotatef(nil, "Invoking cmd.RoundTrip")
	ures, err := cmd.RoundTrip(ctx, desc, conn)
	span.Annotatef(nil, "Finished invoking cmd.RoundTrip")
	if err == nil {
		stats.Record(ctx, observability.MUpdates.M(1))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "update"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return ures, err
}
