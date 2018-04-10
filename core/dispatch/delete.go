package dispatch

import (
	"context"

	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/options"
	"github.com/mongodb/mongo-go-driver/core/result"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
	"github.com/mongodb/mongo-go-driver/internal/trace"
)

// Delete handles the full cycle dispatch and execution of a delete command against the provided
// topology.
func Delete(
	ctx context.Context,
	cmd command.Delete,
	topo *topology.Topology,
	selector description.ServerSelector,
	wc *writeconcern.WriteConcern,
) (result.Delete, error) {

	ctx, span := trace.SpanFromFunctionCaller(ctx)
	defer span.End()

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return result.Delete{}, err
	}

	if wc != nil {
		opt, err := writeConcernOption(wc)
		if err != nil {
			return result.Delete{}, err
		}
		cmd.Opts = append(cmd.Opts, opt)
	}

	// NOTE: We iterate through the options because the user may have provided
	// an option explicitly and that needs to override the provided write concern.
	// We put this here because it would complicate the methods that call this to
	// parse out the option.
	acknowledged := true
	for _, opt := range cmd.Opts {
		wc, ok := opt.(options.OptWriteConcern)
		if !ok {
			continue
		}
		acknowledged = wc.Acknowledged
		break
	}
	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		return result.Delete{}, err
	}

	if !acknowledged {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()
			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()
		return result.Delete{}, ErrUnacknowledgedWrite
	}
	defer conn.Close()

	return cmd.RoundTrip(ctx, desc, conn)
}
