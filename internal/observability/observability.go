package observability

import (
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const by = "By"
const ms = "ms"
const dimensionless = "1"

// Tag keys
var KeyMethod, _ = tag.NewKey("method")
var KeyPart, _ = tag.NewKey("part")

var (
	// MErrors is representative of all errors, differentiated by the tag of the command e.g:
	//   "write", "read", "drop", "decode", "connection", "find", "distinction"
	MErrors = stats.Int64("mongo/client/errors", "The number of errors encountered", dimensionless)

	// MCalls is representative of all calls, differentiated by the tag of the command e.g:
	//   "write", "read", "drop", "decode", "connection", "find", "distinction"
	MCalls = stats.Int64("mongo/client/calls", "The number of call invocations", dimensionless)

	MBytesWritten = stats.Int64("mongo/client/bytes_written", "The number of bytes written", by)
	MBytesRead    = stats.Int64("mongo/client/bytes_read", "The number of bytes read", by)

	MDeletions  = stats.Int64("mongo/client/deletions", "The number of deletions", dimensionless)
	MInsertions = stats.Int64("mongo/client/insertions", "The number of insertions", dimensionless)
	MReads      = stats.Int64("mongo/client/reads", "The number of reads", dimensionless)
	MUpdates    = stats.Int64("mongo/client/updates", "The number of updates", dimensionless)
	MReplaces   = stats.Int64("mongo/client/replaces", "The number of replaces", dimensionless)
	MWrites     = stats.Int64("mongo/client/writes", "The number of writes", dimensionless)

	MConnectionsNew    = stats.Int64("mongo/client/connections_new", "The number of new connections", dimensionless)
	MConnectionsReused = stats.Int64("mongo/client/connections_reused", "The number of reused connections", dimensionless)
	MConnectionsClosed = stats.Int64("mongo/client/connections_closed", "The number of closed connections", dimensionless)

	MConnectionLatencyMilliseconds = stats.Int64("mongo/client/connection_latency", "The latency to make a connection", dimensionless)
	MRoundTripLatencyMilliseconds  = stats.Float64("mongo/client/roundtrip_latency", "The roundtrip latency of commands in milliseconds", ms)
)

var (
	defaultLatencyMillisecondsDistribution = view.Distribution(
		0, 0.5, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 30, 40, 50, 60, 80, 100, 130, 160, 200, 230, 250,
		300, 350, 400, 500, 550, 600, 700, 800, 900, 1000, 1050, 1200, 2000, 2500, 3000, 3500, 4000,
		4500, 5000, 7000, 8000, 10000, 11000, 12000, 15000, 20000, 22000, 25000, 30000, 34000, 40000,
		45000, 50000, 55000, 60000, 65000, 70000, 75000, 80000, 85000, 90000, 95000, 100000, 105000,
		110000, 115000, 120000, 125000, 150000, 200000, 250000, 300000, 400000, 500000, 700000, 800000)

	defaultByteSizesDistribution = view.Distribution(
		0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768,
		65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216,
		33554432, 67108864, 134217728, 268435456, 536870912, 1073741824, 2147483648, 4294967296)
)

var AllViews = []*view.View{
	{
		Name:        "mongo/client/bytes_read",
		Description: "The number of bytes read",
		Measure:     MBytesRead,
		Aggregation: defaultByteSizesDistribution,
	},
	{
		Name:        "mongo/client/bytes_read_count",
		Description: "The number of bytes read",
		Measure:     MBytesRead,
		Aggregation: view.Count(),
	},
	{
		Name:        "mongo/client/bytes_written",
		Description: "The number of bytes written",
		Measure:     MBytesWritten,
		Aggregation: defaultByteSizesDistribution,
	},
	{
		Name:        "mongo/client/bytes_written_count",
		Description: "The number of bytes written",
		Measure:     MBytesWritten,
		Aggregation: view.Count(),
	},
	{
		Name:        "mongo/client/reads",
		Description: "The number of reads",
		Measure:     MReads,
		Aggregation: view.Count(),
	},
	{
		Name:        "mongo/client/writes",
		Description: "The number of writes",
		Measure:     MWrites,
		Aggregation: view.Count(),
	},
	{
		Name:        "mongo/client/roundtrip_latency",
		Description: "The distribution of roundtrip latencies",
		Measure:     MRoundTripLatencyMilliseconds,
		Aggregation: defaultLatencyMillisecondsDistribution,
		TagKeys:     []tag.Key{KeyMethod},
	},
	{
		Name:        "mongo/client/connection_latency",
		Description: "The distribution of connection roundtrip latencies",
		Measure:     MConnectionLatencyMilliseconds,
		Aggregation: defaultLatencyMillisecondsDistribution,
	},

	{
		Name:        "mongo/client/connections_new",
		Description: "The number of new connections",
		Measure:     MConnectionsNew,
		Aggregation: view.Count(),
	},
	{
		Name:        "mongo/client/connections_reused",
		Description: "The number of connections reused or taken from a pool",
		Measure:     MConnectionsReused,
		Aggregation: view.Count(),
	},
	{
		Name:        "mongo/client/connections_closed",
		Description: "The number of connections closed",
		Measure:     MConnectionsClosed,
		Aggregation: view.Count(),
	},

	{
		Name:        "mongo/client/errors",
		Description: "The number of errors during different operations",
		Measure:     MErrors,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{KeyMethod, KeyPart},
	},
	{
		Name:        "mongo/client/calls",
		Description: "The number of calls differentiated by their command names",
		Measure:     MCalls,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{KeyMethod},
	},
}

// Helper functions
func SinceInMilliseconds(startTime time.Time) float64 {
	return time.Since(startTime).Seconds() * 1000
}
