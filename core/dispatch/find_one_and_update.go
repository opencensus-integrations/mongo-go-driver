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

	"go.opencensus.io/trace"
)

// FindOneAndUpdate handles the full cycle dispatch and execution of a FindOneAndUpdate command against the provided
// topology.
func FindOneAndUpdate(
	ctx context.Context,
	cmd command.FindOneAndUpdate,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
) (result.FindAndModify, error) {

	ctx, span := trace.StartSpan(ctx, "mongo-go/core/dispatch.FindOneAndUpdate")
	defer span.End()

	span.Annotatef(nil, "Invoking topology.SelectServer")
	ss, err := topo.SelectServer(ctx, selector)
	span.Annotatef(nil, "Finished invoking topology.SelectServer")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return result.FindAndModify{}, err
	}

	desc := ss.Description()
	span.Annotatef(nil, "Invoking ss.Connection")
	conn, err := ss.Connection(ctx)
	span.Annotatef(nil, "Finished invoking ss.Connection")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return result.FindAndModify{}, err
	}

	if !writeconcern.AckWrite(cmd.WriteConcern) {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()

			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: "Unackwnowledge write"})
		return result.FindAndModify{}, command.ErrUnacknowledgedWrite
	}
	defer conn.Close()

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return result.FindAndModify{}, err
		}
		defer cmd.Session.EndSession()
	}

	span.Annotatef(nil, "Invoking cmd.RoundTrip")
	fim, err := cmd.RoundTrip(ctx, desc, conn)
	span.Annotatef(nil, "Finished invoking cmd.RoundTrip")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return fim, err

}
