package dispatch

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/internal/trace"
)

// DropIndexes handles the full cycle dispatch and execution of a dropIndexes
// command against the provided topology.
func DropIndexes(
	ctx context.Context,
	cmd command.DropIndexes,
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
