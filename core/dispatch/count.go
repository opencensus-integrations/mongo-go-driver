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
	"github.com/mongodb/mongo-go-driver/core/readconcern"
	"github.com/mongodb/mongo-go-driver/core/topology"

	"go.opencensus.io/trace"
)

// Count handles the full cycle dispatch and execution of a count command against the provided
// topology.
func Count(
	ctx context.Context,
	cmd command.Count,
	topo *topology.Topology,
	selector description.ServerSelector,
	rc *readconcern.ReadConcern,
) (int64, error) {

	ctx, span := trace.StartSpan(ctx, "mongo-go/core/dispatch.Count")
	defer span.End()

	span.Annotatef(nil, "Invoking topology.SelectServer")
	ss, err := topo.SelectServer(ctx, selector)
	span.Annotatef(nil, "Finished invoking topology.SelectServer")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return 0, err
	}

	if rc != nil {
		span.Annotatef(nil, "Creating readConcernOption")
		opt, err := readConcernOption(rc)
		span.Annotatef(nil, "Finished creating readConcernOption")
		if err != nil {
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return 0, err
		}
		cmd.Opts = append(cmd.Opts, opt)
	}

	desc := ss.Description()
	span.Annotatef(nil, "Creating Connection")
	conn, err := ss.Connection(ctx)
	span.Annotatef(nil, "Finished creating Connection")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return 0, err
	}
	defer conn.Close()

	cur, err := cmd.RoundTrip(ctx, desc, conn)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return cur, err
}
