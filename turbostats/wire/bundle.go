// Package wire is the TurboStats contract: the document a sqlflow process
// reports about itself, the response a control plane answers with, and the
// signature that authenticates the request.
//
// It is public so a control plane outside this module builds against the same
// types the engine does. It imports only the standard library, so importing
// it pulls in nothing of the engine.
//
// The contract is
// docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md.
// Three rules from it govern every change here:
//
//   - Sections are optional top-level objects. Presence says what the process
//     does, so there is no kind field.
//   - Every reader ignores unknown sections and unknown fields. A new one is
//     additive and stays v1.
//   - A removed or renamed field is v2, at a new path and media type. The rule
//     holds from the first receiver onward. One move and one rename happened
//     inside v1 before any receiver existed; the contract records both.
package wire

import (
	"encoding/json"
	"time"
)

// MediaType names the version on the wire, so v1 and v2 are distinguishable
// without parsing.
const MediaType = "application/vnd.turbolytics.turbostats.v1+json"

// Version is the document version, carried in the body so a bundle stored or
// forwarded without its URL still says what it is.
const Version = 1

// Bundle is one report.
type Bundle struct {
	V      int       `json:"v"`
	SentAt time.Time `json:"sent_at"`
	// IntervalSeconds is the reporter's configured interval. A receiver needs
	// it to tell a late heartbeat from a normal one. Absent when no reporter
	// is configured.
	IntervalSeconds int `json:"interval_seconds,omitempty"`
	// LastActivityAt is the latest of the section activity timestamps. It is
	// a denormalized copy: the section fields are the source. A receiver
	// reads it for staleness without knowing which sections exist.
	LastActivityAt *time.Time `json:"last_activity_at,omitempty"`
	// IdleSeconds is how long since the process last did work, from its
	// monotonic clock. Absent before any work, and from engines that predate
	// the field. A receiver judging work reads this, not LastActivityAt: a
	// wall-clock step between two readings moves their difference, and
	// cannot move this. It excludes time the host was suspended, as
	// UptimeSeconds does.
	IdleSeconds *int64 `json:"idle_seconds,omitempty"`

	Instance Instance `json:"instance"`
	Process  Process  `json:"process"`

	// Pipeline is present when the process runs a consume loop.
	Pipeline *Pipeline `json:"pipeline,omitempty"`
	// Serve is present when the process answers dataset requests.
	Serve *Serve `json:"serve,omitempty"`
	// Freshness is present when the process measures a store's tables: the
	// rollup daemon's leader.
	Freshness *Freshness `json:"freshness,omitempty"`
	// Rollup is present when the process runs rollups.
	Rollup *Rollup `json:"rollup,omitempty"`

	// Commands is a reserved name. v1 never populates it; a later spec
	// defines command status.
	Commands []json.RawMessage `json:"commands,omitempty"`

	// Exit is present only in the last bundle a clean shutdown sends.
	Exit *Exit `json:"exit,omitempty"`
}

// Instance is what the operator and the build said this process is.
type Instance struct {
	// ID is the operator's name for the instance. Empty until a reporter is
	// configured, which requires it.
	ID string `json:"id,omitempty"`
	// Name is pipeline.name under run and serve.name under serve.
	Name       string `json:"name,omitempty"`
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	Arch       string `json:"arch"`
	ConfigHash string `json:"config_hash"`
	// What this instance is made of, from its config. A receiver groups a
	// fleet by these: "every stream that reads from Kafka" is a query
	// rather than a grep. A serve process has none of them and sends none of
	// them.
	SourceType  string `json:"source_type,omitempty"`
	SinkType    string `json:"sink_type,omitempty"`
	HandlerType string `json:"handler_type,omitempty"`
	// Labels are the operator's own, declared in the config and fixed for
	// the life of the process. At most 10, keys [a-z][a-z0-9_]* up to 32
	// characters, values up to 64, and never a name this contract already
	// defines. The bounds are what keep a report a fixed shape and a
	// bounded size.
	Labels map[string]string `json:"labels,omitempty"`
}

// Process is what the runtime and the kernel say about this process.
type Process struct {
	StartedAt time.Time `json:"started_at"`
	// UptimeSeconds is how long this process has run, from its monotonic
	// clock. Absent from engines that predate the field. StartedAt is for
	// display: a gateway that boots near 1970 and then syncs keeps a 1970
	// StartedAt for the life of the process, and its uptime stays right.
	//
	// It is time awake. Go's monotonic clock is CLOCK_MONOTONIC on Linux and
	// mach_absolute_time on macOS, and neither advances while the host is
	// suspended, so a gateway that sleeps for an hour reports an uptime
	// without that hour. It can be less than now minus StartedAt, and that
	// is not a contradiction.
	UptimeSeconds *int64 `json:"uptime_seconds,omitempty"`
	// RSSBytes is absent when the kernel would not answer. A live process
	// never holds zero bytes, so absent and zero cannot be confused.
	//
	// It is omittable rather than required because the alternative was
	// worse: a failed read used to fail the whole bundle, so a process that
	// was running fine reported nothing at all, and a receiver reads silence
	// as a dead instance. Losing one number beats losing the heartbeat.
	RSSBytes   int64 `json:"rss_bytes,omitempty"`
	Goroutines int   `json:"goroutines"`
}

// Pipeline carries the consume loop's totals since Process.StartedAt.
// DurationBounds are the wire's histogram boundaries, in seconds, with a
// ninth bucket for everything above the last.
//
// They live here rather than in the bundle: nine counts carry a
// distribution, and a receiver knows what they mean without being told.
// They are also fixed across a fleet on purpose, because buckets that
// differ per instance cannot be summed.
var DurationBounds = []float64{0.001, 0.01, 0.05, 0.25, 1, 5, 30, 60}

// Duration is how long one kind of work takes.
//
// Count, SumSeconds and Buckets are counters since the process started, so a
// receiver subtracts two reports for an interval's distribution and sums
// buckets across a fleet. MinSeconds and MaxSeconds are since the process
// started and never reset: the reporter and GET /turbostats/v1 both read
// this, and a value reset on read would hide events from the other.
//
// Min and max earn their place beside the buckets because nobody knows a
// customer's workload. One whose every sample lands above the last boundary
// has a distribution that says nothing, and min, max, count and sum stay
// true regardless.
type Duration struct {
	Count      uint64  `json:"count"`
	SumSeconds float64 `json:"sum_seconds"`
	MinSeconds float64 `json:"min_seconds"`
	MaxSeconds float64 `json:"max_seconds"`
	// Buckets is len(DurationBounds)+1 counts, the last for everything above
	// the final boundary.
	Buckets []uint64 `json:"buckets"`
}

// PipelineDurations is how long the pipeline's work takes. A phase that has
// recorded nothing is absent.
type PipelineDurations struct {
	Batch     *Duration `json:"batch,omitempty"`
	SinkFlush *Duration `json:"sink_flush,omitempty"`
}

// ServeDurations is how long the dataset API's work takes.
type ServeDurations struct {
	Request *Duration `json:"request,omitempty"`
}

type Pipeline struct {
	MessageCount int64 `json:"message_count"`
	// MessagePayloadBytes is the bytes of every message value received,
	// the same messages MessageCount counts, so the two divide to a true
	// average message size.
	//
	// Payload, and named so. It excludes keys, headers, framing and TLS, and
	// under Kafka compression the wire carries fewer bytes than this. It is
	// what the pipeline is asked to process, measured the same way for every
	// source. What a constrained or metered link pays is wire bytes, which
	// only some clients expose and which would be a different field.
	//
	// A pointer, so an engine that predates it reads as unknown rather than
	// as a pipeline that received nothing.
	MessagePayloadBytes *int64 `json:"message_payload_bytes,omitempty"`
	HandlerRowsRead     int64  `json:"handler_rows_read"`
	ErrorCount          int64  `json:"error_count"`
	// The phases errors are attributed to. Absent from engines that predate
	// them; zero is a reading, and means nothing failed in that phase.
	//
	// ErrorCount stays the total, and stays authoritative for it: a receiver
	// that reads only it keeps working.
	SourceErrorCount  *int64 `json:"source_error_count,omitempty"`
	HandlerErrorCount *int64 `json:"handler_error_count,omitempty"`
	SinkErrorCount    *int64 `json:"sink_error_count,omitempty"`
	StateErrorCount   *int64 `json:"state_error_count,omitempty"`
	// DLQRows is rows diverted to the dead-letter queue rather than dropped.
	DLQRows *int64 `json:"dlq_rows,omitempty"`
	// LastErrorCode is the engine's code, such as system.sink.unreachable.
	// The message never crosses the wire: it carries the row that failed, a
	// connection string, a customer's data. An operator with the code and
	// the time finds the message in their own logs.
	LastErrorCode *string    `json:"last_error_code,omitempty"`
	LastErrorAt   *time.Time `json:"last_error_at,omitempty"`
	// EventLagSeconds is how far behind the stream the pipeline ran at its
	// last batch: now minus the newest event in it. EventLagMaxSeconds is
	// the worst since the process started.
	//
	// EventLagObservedAt is when that reading was taken. A consumer cut off
	// from its brokers keeps its last reading, so a receiver judges the
	// reading by its age.
	//
	// EventLagBasis is where the event time came from. The engine sends
	// kafka_create_time (the producer's clock, Kafka's default) or
	// kafka_log_append_time (the broker's, where the topic sets
	// message.timestamp.type), both producer or broker to processing;
	// arrival, the moment the record reached this process, which is
	// queueing inside the process and nothing before it; or, for a source
	// told where its event time is, the configured path into the payload,
	// as written. It travels with the number because these measure
	// different spans, and it is an open vocabulary: a reader that meets a
	// name it does not know keeps the reading and declines to compare it,
	// rather than treating
	// the bundle as invalid. All four are absent for a source with no
	// event time.
	EventLagSeconds    *float64   `json:"event_lag_seconds,omitempty"`
	EventLagMaxSeconds *float64   `json:"event_lag_max_seconds,omitempty"`
	EventLagObservedAt *time.Time `json:"event_lag_observed_at,omitempty"`
	EventLagBasis      *string    `json:"event_lag_basis,omitempty"`
	// RecvWaitSeconds is how long the consume loop has spent waiting for
	// input. It separates a pipeline waiting on a quiet source from one
	// saturated by its own work: near the wall clock means the source is
	// quiet, near zero means the engine is the bottleneck.
	RecvWaitSeconds *float64 `json:"recv_wait_seconds,omitempty"`
	// Duration is how long its work takes.
	Duration         *PipelineDurations `json:"duration,omitempty"`
	SinkFlushCount   int64              `json:"sink_flush_count"`
	SinkRowsAccepted int64              `json:"sink_rows_accepted"`
	SinkRowsWritten  int64              `json:"sink_rows_written"`
	StateCommitCount int64              `json:"state_commit_count"`
	// A pointer so a pipeline with no state path omits the field: absent
	// state and empty state are different facts.
	StateDBSizeBytes *int64 `json:"state_db_size_bytes,omitempty"`
	// Absent until the pipeline receives anything, because zero is not a time.
	LastMessageAt *time.Time `json:"last_message_at,omitempty"`

	// SinkRetryCount is every sink write retried after a failure, summed
	// across sinks. An engine that reports it always sends it, zero included.
	//
	// A pointer, so an engine that predates the field reads as unknown. As a
	// plain integer it decoded to zero from a bundle that never carried it,
	// and an old instance whose sink retried constantly read as one that
	// never had.
	SinkRetryCount *int64 `json:"sink_retry_count,omitempty"`

	// Lag is how far behind the source this pipeline is, summarized across
	// every topic and partition it has read.
	//
	// Pointers, not omitempty on a value. A pipeline that has caught up
	// reports zero, and a pipeline with no Kafka source reports nothing, and
	// omitempty on an int64 would render both as an absent field -- turning
	// the healthiest state the system has into the same page as the unknown
	// one.
	//
	// Both aggregates, because either alone misleads: max hides a backlog
	// spread evenly over every partition, and the total hides one stuck
	// partition among many. LagPartitions says how many points they
	// summarize, without which the two cannot be told apart.
	LagMaxMessages   *int64 `json:"lag_max_messages,omitempty"`
	LagTotalMessages *int64 `json:"lag_total_messages,omitempty"`
	LagPartitions    *int   `json:"lag_partitions,omitempty"`
	// LagObservedAt is when the lag above was last measured, and a receiver
	// must read the two together. Lag is measured when a message is
	// processed, so a consumer that stops receiving keeps its last reading,
	// usually zero, while the backlog grows behind it. SentAt minus this is
	// how old the reading is.
	LagObservedAt *time.Time `json:"lag_observed_at,omitempty"`

	// The three window counters travel as a group: all present when this
	// pipeline runs any window, from the moment it starts, and all absent
	// when it runs none.
	//
	// That is what makes LateRowsDropped readable. It counts rows the engine
	// deleted because they arrived after the watermark, which is silent data
	// loss, and an operator has to be able to tell "no rows were dropped"
	// from "nothing here drops rows". Absent says the second; zero says the
	// first.
	//
	// Dropped and reemitted are separate fields because the policy that
	// splits them is an outcome, not a shard: one number loses data and the
	// other does not, and a sum of the two is true of neither. Both are
	// absent if a window reports a policy this contract has no field for,
	// because a count that leaves some rows out is worse than none.
	//
	// The unit is rows of the window table, not source events. The handler's
	// SQL runs before the window sees anything, so a batch of forty late
	// events grouped into one row is one late row here. The two agree only
	// at batch_size 1; above it the ratio is the handler's aggregation, which
	// is a tuning knob and not a loss. An operator comparing this with an
	// input count is comparing different units. A count in events would
	// need the window to know which column carries each row's event count,
	// which nothing declares today.
	LateRowsDropped   *int64 `json:"late_rows_dropped,omitempty"`
	LateRowsReemitted *int64 `json:"late_rows_reemitted,omitempty"`
	WindowClosedCount *int64 `json:"window_closed_count,omitempty"`

	// WindowLagSeconds is how far the most behind window's closes trail the
	// data it holds, in event seconds: where its watermark should be, given
	// its rows, minus where it is. It is zero while closes keep up, zero
	// after an idle close has closed everything, and it grows while rows
	// arrive and closes fail. Present once any window has polled.
	//
	// No clock enters it, so it carries no skew and a sparse stream does not
	// read as stalled. A stream going quiet is the source's to report, as
	// last_message_at does. Two earlier readings measured against this
	// host's clock and got both wrong: the watermark's age trailed by size
	// and grace by design, and wall time past the next close grew for any
	// stream that paused.
	WindowLagSeconds *int64 `json:"window_lag_seconds,omitempty"`

	// WindowNewestBucketAt is the start of the newest bucket any window
	// holds, in event time: a timestamp from the data, not from this host.
	//
	// Ahead of now means rows are stamped in the future. That is a fault,
	// not an artefact: one device with a fast clock moves the watermark past
	// every correctly-timed row, and under a drop policy each of those is
	// then late and deleted. The engine does not subtract this host's clock
	// from it, because on a gateway without a real-time clock that clock is
	// the thing most likely to be wrong. A receiver compares it with its own
	// clock at receipt.
	WindowNewestBucketAt *time.Time `json:"window_newest_bucket_at,omitempty"`
}

// Serve carries the dataset API's totals since Process.StartedAt.
type Serve struct {
	RequestCount int64 `json:"request_count"`
	// RequestErrorCount counts 5xx answers only. A 4xx is the caller's error.
	RequestErrorCount int64 `json:"request_error_count"`
	// Duration is how long a request takes, end to end.
	Duration      *ServeDurations `json:"duration,omitempty"`
	SessionsInUse int             `json:"sessions_in_use"`
	SessionsTotal int             `json:"sessions_total"`
	// Absent until the server answers a dataset request.
	LastRequestAt *time.Time `json:"last_request_at,omitempty"`
	// Cache is absent on a server where no dataset opted into the cache.
	Cache *ServeCache `json:"cache,omitempty"`
}

// ServeCache carries the response cache's totals and its current size.
type ServeCache struct {
	HitCount      int64 `json:"hit_count"`
	MissCount     int64 `json:"miss_count"`
	SharedCount   int64 `json:"shared_count"`
	EvictionCount int64 `json:"eviction_count"`
	Bytes         int64 `json:"bytes"`
	Entries       int   `json:"entries"`
}

// Freshness is how recent the data in one store's tables is, as this
// process read it. A receiver keys a table by StoreID and Table, so a table
// two processes report is one table, and the newer ObservedAt wins.
type Freshness struct {
	// StoreID names the database without its host or credentials: "pg:"
	// and 16 hex characters of a hash of the server's system identifier and
	// the database's name, the same for every reporter.
	StoreID string `json:"store_id"`
	// StoreIDKind is "system", or "address" when the server refused its
	// system identifier and the id came from the address it answered on,
	// which another reporter may see differently.
	StoreIDKind string `json:"store_id_kind"`
	// ObservedAt is the database's clock when the tables were read.
	ObservedAt time.Time `json:"observed_at"`
	// Tables is one entry per table. The reporter's config bounds it: a
	// rollups file that reports declares at most ten tables, and each rollup
	// adds its source.
	Tables []FreshTable `json:"tables"`
}

// FreshTable is one table's newest bucket. A receiver computes its age as
// ObservedAt minus the bucket's end, its start plus GrainSeconds. A table
// still filling its open bucket reads negative, which is current.
type FreshTable struct {
	// Table is qualified by its schema.
	Table        string `json:"table"`
	GrainSeconds int64  `json:"grain_seconds"`
	// NewestBucketAt is the start of the newest bucket, absent for an empty
	// table.
	NewestBucketAt *time.Time `json:"newest_bucket_at,omitempty"`
}

// Rollup is what a rollup daemon reports about the rollups it manages.
type Rollup struct {
	// Role is "leader", "standby" or "starting". Only the leader reports
	// the rollups: a standby checks nothing.
	Role string `json:"role"`
	// Rollups is one entry per rollup the file declares. Every rollup
	// declares a table, so the cap on tables bounds it.
	Rollups []RollupEntry `json:"rollups,omitempty"`
	// The code and time of the last error the daemon counted. The message
	// stays in the process: it can carry a DSN.
	LastErrorCode *string    `json:"last_error_code,omitempty"`
	LastErrorAt   *time.Time `json:"last_error_at,omitempty"`
}

// RollupEntry is one rollup's totals since the process started.
type RollupEntry struct {
	Name string `json:"name"`
	// Strategy is how the store keeps the rollup current: "trigger" in v1.
	Strategy string `json:"strategy"`
	// Backfill is absent once every table of the rollup is filled.
	Backfill          *RollupBackfill `json:"backfill,omitempty"`
	VerifyBucketCount int64           `json:"verify_bucket_count"`
	DriftBucketCount  int64           `json:"drift_bucket_count"`
	// Completeness is absent for a rollup with no count_buckets measure,
	// and before any of its tables has a closed bucket.
	Completeness *RollupCompleteness `json:"completeness,omitempty"`
	// Triggers is absent unless the server tracks function calls, which
	// takes track_functions set to pl or all.
	Triggers *RollupTriggers `json:"triggers,omitempty"`
}

// RollupBackfill is how much of a rollup is still filling.
type RollupBackfill struct {
	TablesLeft int `json:"tables_left"`
	// HistoryLeftSeconds is the source history still to fill in the table
	// furthest behind, absent before that table's first chunk.
	HistoryLeftSeconds *int64 `json:"history_left_seconds,omitempty"`
}

// RollupCompleteness is the least complete of the newest closed buckets of
// a rollup's count_buckets tables: the source buckets it holds, against the
// number its width holds. "57 of 60" is an hour missing three minutes.
type RollupCompleteness struct {
	Table           string    `json:"table"`
	BucketAt        time.Time `json:"bucket_at"`
	SourceBuckets   int64     `json:"source_buckets"`
	ExpectedBuckets int64     `json:"expected_buckets"`
}

// RollupTriggers sums the calls and time of a rollup's trigger functions,
// from the server's function statistics.
type RollupTriggers struct {
	Calls        int64   `json:"calls"`
	TotalSeconds float64 `json:"total_seconds"`
}

// Exit is how a clean shutdown ended.
type Exit struct {
	Reason string `json:"reason"`
	Code   int    `json:"code"`
}

// Response is the body of every 2xx answer to a heartbeat.
//
// Commands is a reserved name. A v1 reporter ignores its contents, and must
// still parse a response that carries some: instances deployed today have to
// survive the day a control plane starts sending commands.
type Response struct {
	V        int               `json:"v"`
	Commands []json.RawMessage `json:"commands"`
}
