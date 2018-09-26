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

// Delete handles the full cycle dispatch and execution of a delete command against the provided
// topology.
func Delete(
	ctx context.Context,
	cmd command.Delete,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
	retryWrite bool,
) (result.Delete, error) {

	ctx, span := trace.StartSpan(ctx, "mongo-go/core/dispatch.Delete")
	ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyMethod, "delete"))
	defer span.End()

	span.Annotatef(nil, "Invoking topology.SelectServer")
	ss, err := topo.SelectServer(ctx, selector)
	span.Annotatef(nil, "Finished invoking topology.SelectServer")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "connection"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return result.Delete{}, err
	}

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() && writeconcern.AckWrite(cmd.WriteConcern) {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return result.Delete{}, err
		}
		defer cmd.Session.EndSession()
	}

	// Execute in a single trip if retry writes not supported, or retry not enabled
	if !retrySupported(topo, ss.Description(), cmd.Session, cmd.WriteConcern) || !retryWrite {
		if cmd.Session != nil {
			cmd.Session.RetryWrite = false // explicitly set to false to prevent encoding transaction number
		}
		return delete(ctx, span, cmd, ss, nil)
	}

	cmd.Session.RetryWrite = retryWrite
	cmd.Session.IncrementTxnNumber()

	res, originalErr := delete(ctx, span, cmd, ss, nil)

	// Retry if appropriate
	if cerr, ok := originalErr.(command.Error); ok && cerr.Retryable() ||
		res.WriteConcernError != nil && command.IsWriteConcernErrorRetryable(res.WriteConcernError) {
		ss, err := topo.SelectServer(ctx, selector)

		// Return original error if server selection fails or new server does not support retryable writes
		if err != nil || !retrySupported(topo, ss.Description(), cmd.Session, cmd.WriteConcern) {
			return res, originalErr
		}

		return delete(ctx, span, cmd, ss, cerr)
	}
	return res, originalErr
}

func delete(
	ctx context.Context,
	span *trace.Span,
	cmd command.Delete,
	ss *topology.SelectedServer,
	oldErr error,
) (result.Delete, error) {
	desc := ss.Description()

	span.Annotatef(nil, "Creating ss.Connection")
	conn, err := ss.Connection(ctx)
	span.Annotatef(nil, "Finished creating ss.Connection")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "connection"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		if oldErr != nil {
			return result.Delete{}, oldErr
		}
		return result.Delete{}, err
	}

	if !writeconcern.AckWrite(cmd.WriteConcern) {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()

			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()

		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "write"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: "Unacknowledged write"})
		return result.Delete{}, command.ErrUnacknowledgedWrite
	}
	defer conn.Close()

	di, err := cmd.RoundTrip(ctx, desc, conn)
	span.Annotatef(nil, "Finished invoking cmd.RoundTrip")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "delete"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return di, err
}
