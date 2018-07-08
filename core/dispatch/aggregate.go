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
	"github.com/mongodb/mongo-go-driver/core/option"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"

	"github.com/mongodb/mongo-go-driver/internal/observability"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// Aggregate handles the full cycle dispatch and execution of an aggregate command against the provided
// topology.
func Aggregate(
	ctx context.Context,
	cmd command.Aggregate,
	topo *topology.Topology,
	readSelector, writeSelector description.ServerSelector,
	wc *writeconcern.WriteConcern,
) (command.Cursor, error) {

	ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyMethod, "aggregate"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/core/dispatch.Aggregate")
	defer span.End()

	dollarOut := cmd.HasDollarOut()

	var ss *topology.SelectedServer
	var err error
	acknowledged := true
	switch dollarOut {
	case true:
		span.Annotatef(nil, "Invoking topology.SelectServer")
		ss, err = topo.SelectServer(ctx, writeSelector)
		span.Annotatef(nil, "Finished invoking topology.SelectServer")
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "topo_selectserver"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}
		if wc != nil {
			opt := option.OptWriteConcern{WriteConcern: wc}
			cmd.Opts = append(cmd.Opts, opt)
			acknowledged = wc.Acknowledged()
		}
	case false:
		span.Annotatef(nil, "Invoking topology.SelectServer")
		ss, err = topo.SelectServer(ctx, readSelector)
		span.Annotatef(nil, "Finished invoking topology.SelectServer")
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "topo_selectserver"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}
	}

	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "connection"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if !acknowledged {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()
			_, _ = cmd.RoundTrip(ctx, desc, ss, conn)
		}()
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "write"))
		stats.Record(ctx, observability.MErrors.M(1))
		return nil, ErrUnacknowledgedWrite
	}
	defer conn.Close()

	span.Annotatef(nil, "Invoking cmd.RoundTrip")
	cur, err := cmd.RoundTrip(ctx, desc, ss, conn)
	span.Annotatef(nil, "Finished invoking cmd.RoundTrip")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "roundtrip"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return cur, err
}
