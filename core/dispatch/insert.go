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
	wc *writeconcern.WriteConcern,
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

	acknowledged := true
	if wc != nil {
		opt, err := writeConcernOption(wc)
		if err != nil {
			return result.Insert{}, err
		}
		cmd.Opts = append(cmd.Opts, opt)
		acknowledged = wc.Acknowledged()
	}

	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		return result.Insert{}, err
	}

	if !acknowledged {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()
			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: "Unacknowledged write"})
		return result.Insert{}, ErrUnacknowledgedWrite
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