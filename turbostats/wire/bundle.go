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
	// cannot move this.
	IdleSeconds *int64 `json:"idle_seconds,omitempty"`

	Instance Instance `json:"instance"`
	Process  Process  `json:"process"`

	// Pipeline is present when the process runs a consume loop.
	Pipeline *Pipeline `json:"pipeline,omitempty"`
	// Serve is present when the process answers dataset requests.
	Serve *Serve `json:"serve,omitempty"`

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
}

// Process is what the runtime and the kernel say about this process.
type Process struct {
	StartedAt time.Time `json:"started_at"`
	// UptimeSeconds is how long this process has run, from its monotonic
	// clock. Absent from engines that predate the field. StartedAt is for
	// display: a gateway that boots near 1970 and then syncs keeps a 1970
	// StartedAt for the life of the process, and its uptime stays right.
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
	SinkFlushCount      int64  `json:"sink_flush_count"`
	SinkRowsAccepted    int64  `json:"sink_rows_accepted"`
	SinkRowsWritten     int64  `json:"sink_rows_written"`
	StateCommitCount    int64  `json:"state_commit_count"`
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
	SessionsInUse     int   `json:"sessions_in_use"`
	SessionsTotal     int   `json:"sessions_total"`
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
