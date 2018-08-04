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
	"github.com/mongodb/mongo-go-driver/core/topology"

	"github.com/mongodb/mongo-go-driver/internal/observability"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// EndSessions handles the full cycle dispatch and execution of an endSessions command against the provided
// topology.
func EndSessions(
	ctx context.Context,
	cmd command.EndSessions,
	topo *topology.Topology,
	selector description.ServerSelector,
) ([]result.EndSessions, []error) {

	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "command"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/core/dispatch.Command")
	defer span.End()

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "topo_selectserver"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, []error{err}
	}

	conn, err := ss.Connection(ctx)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "connection"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, []error{err}
	}

	br, errs := cmd.RoundTrip(ctx, ss.Description(), conn)
	if len(errs) != 0 {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "roundtrip"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: errs[0].Error()})
	}
	return br, errs

}
