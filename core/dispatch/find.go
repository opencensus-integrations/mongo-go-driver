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

// Find handles the full cycle dispatch and execution of a find command against the provided
// topology.
func Find(
	ctx context.Context,
	cmd command.Find,
	topo *topology.Topology,
	selector description.ServerSelector,
	rc *readconcern.ReadConcern,
) (command.Cursor, error) {

	ctx, span := trace.StartSpan(ctx, "mongo-go/core/dispatch.Find")
	defer span.End()

	span.Annotatef(nil, "Invoking topology.SelectServer")
	ss, err := topo.SelectServer(ctx, selector)
	span.Annotatef(nil, "Finished invoking topology.SelectServer")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}

	if rc != nil {
		span.Annotatef(nil, "Creating readConcernOption")
		opt, err := readConcernOption(rc)
		span.Annotatef(nil, "Finished creating readConcernOption")
		if err != nil {
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, err
		}
		cmd.Opts = append(cmd.Opts, opt)
	}

	desc := ss.Description()
	span.Annotatef(nil, "Invoking ss.Connection")
	conn, err := ss.Connection(ctx)
	span.Annotatef(nil, "Finished invoking ss.Connection")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, err
	}
	defer conn.Close()

	span.Annotatef(nil, "Invoking cmd.RoundTrip")
	cur, err := cmd.RoundTrip(ctx, desc, ss, conn)
	span.Annotatef(nil, "Finished invoking cmd.RoundTrip")
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return cur, err
}
