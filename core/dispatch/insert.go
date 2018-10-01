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

// Insert handles the full cycle dispatch and execution of an insert command against the provided
// topology.
func Insert(
	ctx context.Context,
	cmd command.Insert,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
	retryWrite bool,
) (result.Insert, error) {

	ctx, span := trace.StartSpan(ctx, "mongo-go/core/dispatch.Insert")
	defer span.End()

	span.Annotatef(nil, "Invoking topology.SelectServer")
	ss, err := topo.SelectServer(ctx, selector)
	span.Annotatef(nil, "Finished invoking topology.SelectServer")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return result.Insert{}, err
	}

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return result.Insert{}, err
		}
		defer cmd.Session.EndSession()
	}

	// Execute in a single trip if retry writes not supported, or retry not enabled
	if !retrySupported(topo, ss.Description(), cmd.Session, cmd.WriteConcern) || !retryWrite {
		if cmd.Session != nil {
			cmd.Session.RetryWrite = false // explicitly set to false to prevent encoding transaction number
		}
		return insert(ctx, span, cmd, ss, nil)
	}

	// TODO figure out best place to put retry write.  Command shouldn't have to know about this field.
	cmd.Session.RetryWrite = retryWrite
	cmd.Session.IncrementTxnNumber()

	res, originalErr := insert(ctx, span, cmd, ss, nil)

	// Retry if appropriate
	if cerr, ok := originalErr.(command.Error); ok && cerr.Retryable() ||
		res.WriteConcernError != nil && command.IsWriteConcernErrorRetryable(res.WriteConcernError) {
		ss, err := topo.SelectServer(ctx, selector)

		// Return original error if server selection fails or new server does not support retryable writes
		if err != nil || !retrySupported(topo, ss.Description(), cmd.Session, cmd.WriteConcern) {
			return res, originalErr
		}

		return insert(ctx, span, cmd, ss, cerr)
	}

	return res, originalErr
}

func insert(
	ctx context.Context,
	span *trace.Span,
	cmd command.Insert,
	ss *topology.SelectedServer,
	oldErr error,
) (result.Insert, error) {
	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		if oldErr != nil {
			return result.Insert{}, oldErr
		}
		return result.Insert{}, err
	}

	if !writeconcern.AckWrite(cmd.WriteConcern) {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()

			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()

		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: "Unacknowledged write"})
		return result.Insert{}, command.ErrUnacknowledgedWrite
	}
	defer conn.Close()

	span.Annotatef(nil, "Invoking command.RoundTrip")
	ri, err := cmd.RoundTrip(ctx, desc, conn)
	span.Annotatef(nil, "Finished invoking command.RoundTrip")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return ri, err
}
