package wire

import "time"

// Database is the section a dbhealth reporter sends: one database endpoint,
// probed and measured. Facts only: timestamps, counts and limits. The
// receiver judges them. Fields a kind cannot provide are absent, not zero.
//
// Spec: docs/superpowers/specs/2026-10-05-turbostats-database-section-design.md.
type Database struct {
	// Kind is postgres, mysql, mongo, snowflake or redshift.
	Kind string `json:"kind"`
	// Target is host:port/database. Never a DSN: it carries no credential.
	Target string `json:"target"`
	// Cluster groups a primary with its replicas. Absent when not configured.
	Cluster       string `json:"cluster,omitempty"`
	ServerVersion string `json:"server_version,omitempty"`

	// Probe is always present. When it failed, nothing below it is.
	Probe DatabaseProbe `json:"probe"`

	Resources   *DatabaseResources   `json:"resources,omitempty"`
	Tables      []DatabaseTable      `json:"tables,omitempty"`
	Replication *DatabaseReplication `json:"replication,omitempty"`
	Collection  DatabaseCollection   `json:"collection"`
}

// DatabaseProbe is one round trip per interval: is it serving, and how fast.
type DatabaseProbe struct {
	OK        bool       `json:"ok"`
	LatencyMs int64      `json:"latency_ms"`
	LastOKAt  *time.Time `json:"last_ok_at,omitempty"`
	// ConsecutiveFailures counts probes failed in a row, 0 when OK.
	ConsecutiveFailures int `json:"consecutive_failures"`
	// Error is the class of the latest failure: refused, timeout, auth or
	// other. Empty when OK.
	Error string `json:"error,omitempty"`
}

// DatabaseResources is how close the database is to its limits. Each field
// is absent when the kind, or the role, cannot read it.
type DatabaseResources struct {
	Connections *DatabaseConnections `json:"connections,omitempty"`
	// SizeBytes is this database on disk.
	SizeBytes *int64 `json:"size_bytes,omitempty"`
	// OldestTransactionSeconds is the age of the oldest open transaction;
	// one idle in transaction holds vacuum back.
	OldestTransactionSeconds *int64          `json:"oldest_transaction_seconds,omitempty"`
	Memory                   *DatabaseMemory `json:"memory,omitempty"`
}

// DatabaseConnections is connections in use against the configured maximum.
type DatabaseConnections struct {
	Used    int `json:"used"`
	Max     int `json:"max"`
	Waiting int `json:"waiting"`
}

// DatabaseMemory carries settings, not usage: a database does not know the
// host's memory from SQL.
type DatabaseMemory struct {
	SharedBuffersBytes int64 `json:"shared_buffers_bytes"`
}

// DatabaseTable is one watched table's freshness, size and row count.
type DatabaseTable struct {
	Name            string `json:"name"`
	FreshnessColumn string `json:"freshness_column,omitempty"`
	// NewestAt is max(FreshnessColumn). Absent when the table has none.
	NewestAt *time.Time `json:"newest_at,omitempty"`
	// Rows is an estimate unless RowsExact, which says count(*) ran.
	Rows         *int64     `json:"rows,omitempty"`
	RowsExact    bool       `json:"rows_exact"`
	SizeBytes    int64      `json:"size_bytes"`
	LastVacuumAt *time.Time `json:"last_vacuum_at,omitempty"`
	CheckedAt    time.Time  `json:"checked_at"`
}

// DatabaseReplication is measured from this endpoint's side. A replica
// reports how far behind what it serves is; a primary reports what it knows
// about each replica.
type DatabaseReplication struct {
	// Role is primary or replica.
	Role           string     `json:"role"`
	LagSeconds     *float64   `json:"lag_seconds,omitempty"`
	LastReplayedAt *time.Time `json:"last_replayed_at,omitempty"`
	Upstream       string     `json:"upstream,omitempty"`
	// Replicas is a primary's view of each replica.
	Replicas []DatabaseReplica `json:"replicas,omitempty"`
}

// DatabaseReplica is one replica as its primary sees it.
type DatabaseReplica struct {
	Name       string   `json:"name"`
	LagSeconds *float64 `json:"lag_seconds,omitempty"`
	State      string   `json:"state,omitempty"`
}

// DatabaseCollection is what the interval cost, so the receiver can show
// the price of watching, and every per-table failure, so one table the
// role cannot read does not hide the others.
type DatabaseCollection struct {
	Queries    int             `json:"queries"`
	DurationMs int64           `json:"duration_ms"`
	Errors     []DatabaseError `json:"errors"`
}

// DatabaseError is one query that failed this interval.
type DatabaseError struct {
	Table string `json:"table,omitempty"`
	Error string `json:"error"`
}
