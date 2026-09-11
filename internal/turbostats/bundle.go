// Package turbostats is the document a sqlflow process reports about itself.
//
// One bundle, built by one function, carried by two transports: the HTTP
// handler in this package serves it to whatever can reach the process, and
// the reporter posts it outbound to a control plane that cannot. Neither
// transport computes anything.
//
// Field names are the OTel instrument names in internal/core, not the
// Prometheus series names those instruments render as. The bundle reads the
// instrument; the suffixes belong to one exporter.
package turbostats

import "time"

// MediaType names the version on the wire, so v1 and v2 are distinguishable
// without parsing.
const MediaType = "application/vnd.turbolytics.turbostats.v1+json"

// Version is the document version, carried in the body so a bundle stored or
// forwarded without its URL still says what it is.
const Version = 1

// Bundle is one report. The spec at
// docs/superpowers/specs/2026-09-10-turbostats-v1-design.md defines every
// field; the comments here say only what is not obvious from the name.
type Bundle struct {
	V        int       `json:"v"`
	SentAt   time.Time `json:"sent_at"`
	Instance Instance  `json:"instance"`
	Process  Process   `json:"process"`
	Pipeline Pipeline  `json:"pipeline"`
	// LastMessageAt is when the pipeline last received messages. Absent until
	// it receives any. Staleness is sent_at minus this, and because both come
	// from the instance's own clock the difference carries no skew.
	LastMessageAt *time.Time `json:"last_message_at,omitempty"`
	// Exit is present only in the last bundle a clean shutdown sends. A
	// bundle without it is an instance still running, or one that died
	// without saying so.
	Exit *Exit `json:"exit,omitempty"`
}

// Instance is what the operator and the build said this process is.
type Instance struct {
	// ID is the operator's name for the instance. Empty until the reporter
	// config exists, which is why it is omitempty here and required there.
	ID         string `json:"id,omitempty"`
	Pipeline   string `json:"pipeline,omitempty"`
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	Arch       string `json:"arch"`
	ConfigHash string `json:"config_hash"`
}

// Process is what the runtime and the kernel say about this process.
type Process struct {
	StartedAt  time.Time `json:"started_at"`
	RSSBytes   int64     `json:"rss_bytes"`
	Goroutines int       `json:"goroutines"`
}

// Pipeline carries the engine's top-line totals since Process.StartedAt.
//
// Every one is read from a dimensionless series, so building this is a
// lookup. Flushes and commits count successes only, and the row counts
// exclude the DLQ's, because those choices are made where the measurements
// are recorded rather than here. Histograms are deliberately absent: they are
// most of a scrape by bytes and nothing on the first page reads them.
type Pipeline struct {
	MessageCount     int64 `json:"message_count"`
	HandlerRowsRead  int64 `json:"handler_rows_read"`
	ErrorCount       int64 `json:"error_count"`
	SinkFlushCount   int64 `json:"sink_flush_count"`
	SinkRowsAccepted int64 `json:"sink_rows_accepted"`
	SinkRowsWritten  int64 `json:"sink_rows_written"`
	StateCommitCount int64 `json:"state_commit_count"`
	// A pointer so a pipeline with no state path omits the field: absent
	// state and empty state are different facts.
	StateDBSizeBytes *int64 `json:"state_db_size_bytes,omitempty"`
}

// Exit is how a clean shutdown ended.
type Exit struct {
	Reason string `json:"reason"`
	Code   int    `json:"code"`
}

// Static is what the run command knows once, at startup, and the package
// cannot learn on its own.
type Static struct {
	ID, Pipeline, Version, Commit, ConfigHash string
	StartedAt                                 time.Time
}
