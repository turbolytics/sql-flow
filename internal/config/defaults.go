package config

// Defaults that more than one package reads. They live here, beside the
// fields they fill in, because `sqlflow validate` compares them and must not
// import the packages that act on them: internal/sinks and internal/core link
// DuckDB, and validate is the check that runs without it.

// DefaultDrainDeadlineSeconds is pipeline.drain_deadline_seconds when the
// config omits it. Thirty is Kubernetes' default
// terminationGracePeriodSeconds, so a pipeline with no setting still finishes
// or fails its drain before the supervisor stops waiting.
const DefaultDrainDeadlineSeconds = 30

// The sink retry ladder's defaults, for the fields of SinkRetry the config
// omits.
const (
	DefaultSinkRetryMaxAttempts     = 4
	DefaultSinkRetryDeadlineSeconds = 10
)

// SinkRetries reports whether a sink type is wrapped in a retry ladder.
//
// Only the sinks that cross a network to somebody else's server. Kafka is
// excluded on purpose: it hands records to franz-go, which already retries a
// produce with its own backoff, and a second ladder on top of that one delays
// the report without improving delivery. Console, noop and sqlcommand reach
// nothing that can be temporarily unavailable -- sqlcommand writes through the
// pipeline's own DuckDB connection, and a failure there is not a blip.
func SinkRetries(sinkType string) bool {
	switch sinkType {
	case "clickhouse", "iceberg":
		return true
	default:
		return false
	}
}
