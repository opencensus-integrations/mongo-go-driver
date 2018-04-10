package dispatch

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/internal/trace"
)

// Command handles the full cycle dispatch and execution of a command against the provided
// topology.
func Command(
	ctx context.Context,
	cmd command.Command,
	topo *topology.Topology,
	selector topology.ServerSelector,
) (bson.Reader, error) {

	ctx, span := trace.SpanFromFunctionCaller(ctx)
	defer span.End()

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return nil, err
	}

	conn, err := ss.Connection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return cmd.RoundTrip(ctx, ss.Description(), conn)
}
