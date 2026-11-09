package wire

import "time"

// MaxDatabaseTables bounds Database.Tables. A reporter watches at most this
// many tables per endpoint; its config refuses more. It is the one number
// that sets a database bundle's width, so it is in the contract: the widest
// bundle of MaxDatabaseTables tables with the longest names Postgres allows
// is priced by TestCollect_AFullDatabaseBundleStaysUnderTheCeiling, and the
// receiver's body limit has to hold it.
const MaxDatabaseTables = 50

// MaxDatabaseReplicas bounds DatabaseReplication.Replicas. A primary with
// more sends the first MaxDatabaseReplicas by name.
const MaxDatabaseReplicas = 16

// MaxDatabaseReplicationSlots bounds DatabaseReplication.Slots. A server with
// more sends the slots that hold the most WAL, because those fill the disk
// first.
const MaxDatabaseReplicationSlots = 32

// MaxDatabaseErrors bounds DatabaseCollection.Errors: one per watched table
// and one per catalog query, which is fewer than ten.
const MaxDatabaseErrors = MaxDatabaseTables + 10

// MaxDatabaseQueries bounds Database.Queries, the opt-in per-query facts:
// the reporter sends at most this many, by share of time.
const MaxDatabaseQueries = 20

// MaxDatabaseQueryText bounds DatabaseQuery.Text, in bytes. The text is
// the system's normalized form, literals replaced, and truncated.
const MaxDatabaseQueryText = 200

// MaxDatabaseSchemaChanges bounds the DatabaseTable.SchemaChanges across a
// bundle: a reporter sends at most this many in one report, by table and
// then by column, and the rest in the next. A migration touches a handful
// of columns on a few tables; sixty is a schema rewrite.
const MaxDatabaseSchemaChanges = 60

// MaxDatabaseTypeText bounds DatabaseSchemaChange.From and To, in bytes:
// a column type as the system names it, truncated.
const MaxDatabaseTypeText = 64

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
	// Load is what people are doing to the database. Present when the kind
	// provides at least one field.
	Load *DatabaseLoad `json:"load,omitempty"`
	// Queries is the work by query, opt-in, at most MaxDatabaseQueries,
	// ordered by TimeShare descending.
	Queries    []DatabaseQuery    `json:"queries,omitempty"`
	Collection DatabaseCollection `json:"collection"`
}

// DatabaseLoad is what people are doing to the database. Samples, named
// _now, are the instant of collection; rates, named _per_second, are the
// interval's work computed from the system's own counters two readings
// apart, absent on the first interval and after a counter reset, never
// zero in those cases. Every field is optional: a kind sends what it has.
//
// Spec: docs/superpowers/specs/2026-10-06-turbostats-database-load-design.md.
type DatabaseLoad struct {
	// SessionsActiveNow is sessions executing a statement.
	SessionsActiveNow            *int `json:"sessions_active_now,omitempty"`
	SessionsIdleInTransactionNow *int `json:"sessions_idle_in_transaction_now,omitempty"`
	// SessionsWaitingNow is sessions blocked: on a lock, a queue, a resource.
	SessionsWaitingNow *int `json:"sessions_waiting_now,omitempty"`
	// QueriesQueuedNow is queries waiting to start; warehouses and WLM.
	QueriesQueuedNow *int `json:"queries_queued_now,omitempty"`
	// LongestQuerySeconds is the age of the oldest statement still running.
	LongestQuerySeconds *float64 `json:"longest_query_seconds,omitempty"`

	QueriesPerSecond      *float64 `json:"queries_per_second,omitempty"`
	TransactionsPerSecond *float64 `json:"transactions_per_second,omitempty"`
	RollbacksPerSecond    *float64 `json:"rollbacks_per_second,omitempty"`
	// RowsReadPerSecond is rows returned to clients.
	RowsReadPerSecond *float64 `json:"rows_read_per_second,omitempty"`
	// RowsWrittenPerSecond is inserted, updated and deleted.
	RowsWrittenPerSecond *float64 `json:"rows_written_per_second,omitempty"`
	// BytesScannedPerSecond is storage read to answer queries.
	BytesScannedPerSecond *float64 `json:"bytes_scanned_per_second,omitempty"`
	// CacheHitRatio is the interval's block or page reads served from
	// memory, 0..1; not since the server started.
	CacheHitRatio      *float64 `json:"cache_hit_ratio,omitempty"`
	DeadlocksPerSecond *float64 `json:"deadlocks_per_second,omitempty"`
	// TempBytesPerSecond is sorts and hashes spilling to disk.
	TempBytesPerSecond *float64 `json:"temp_bytes_per_second,omitempty"`
	// WALBytesPerSecond is the write-ahead log the server wrote. Compared
	// with a slot's RetainedBytes, it says whether a slot keeps up.
	WALBytesPerSecond *float64 `json:"wal_bytes_per_second,omitempty"`
}

// DatabaseQuery is one query shape's share of the interval's work.
type DatabaseQuery struct {
	// ID is the system's query id, or a hash of the text, stable across
	// intervals for the same shape.
	ID string `json:"id"`
	// Text is the normalized form, literals replaced, at most
	// MaxDatabaseQueryText bytes.
	Text           string  `json:"text"`
	CallsPerSecond float64 `json:"calls_per_second"`
	MeanMs         float64 `json:"mean_ms"`
	// TimeShare is this shape's share of all query time in the interval, 0..1.
	TimeShare   float64 `json:"time_share"`
	RowsPerCall float64 `json:"rows_per_call"`
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
	// TableCount is the tables in the database's own schemas, partitions
	// included; the system catalogs are not counted. PartitionCount is how
	// many of them are partitions. A table partitioned by day adds one a
	// day until retention drops one, so a PartitionCount that keeps
	// climbing is retention that stopped.
	TableCount     *int `json:"table_count,omitempty"`
	PartitionCount *int `json:"partition_count,omitempty"`
	// WALBytes is the write-ahead log on disk for the whole server, which
	// SizeBytes does not count. A replication slot that stops advancing
	// grows it until the disk fills.
	WALBytes *int64 `json:"wal_bytes,omitempty"`
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
	// DeadRows is rows deleted or updated and not yet reclaimed.
	DeadRows *int64 `json:"dead_rows,omitempty"`
	// SeqScansPerSecond is whole-table reads; IndexScansPerSecond the rest.
	// Rates over the interval, absent on the first and after a reset.
	SeqScansPerSecond   *float64 `json:"seq_scans_per_second,omitempty"`
	IndexScansPerSecond *float64 `json:"index_scans_per_second,omitempty"`
	// SchemaHash is a hash of the table's columns: each name, type and
	// nullability, in column order. The same every report until the
	// schema moves; a receiver that sees it change knows the table changed
	// shape, and SchemaChanges, sent in that report alone, says how. A
	// reporter that has no previous reading, having just started, sends
	// the hash without changes.
	SchemaHash    string                 `json:"schema_hash,omitempty"`
	SchemaChanges []DatabaseSchemaChange `json:"schema_changes,omitempty"`
	// RowsInsertedPerSecond, RowsUpdatedPerSecond and RowsDeletedPerSecond
	// are the table's writes over the interval from the system's own
	// counters: what the table's volume did, exactly, not what an estimate
	// says it is. Absent on the first interval and after a reset.
	RowsInsertedPerSecond *float64 `json:"rows_inserted_per_second,omitempty"`
	RowsUpdatedPerSecond  *float64 `json:"rows_updated_per_second,omitempty"`
	RowsDeletedPerSecond  *float64 `json:"rows_deleted_per_second,omitempty"`
}

// DatabaseSchemaChange is one column's change between two readings of a
// table's schema: added, dropped, retyped or nullability. From and To are
// the type as the system names it; absent where there was none, and for
// nullability "NOT NULL" or "NULL".
type DatabaseSchemaChange struct {
	Column string `json:"column"`
	Change string `json:"change"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
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
	// Slots is a primary's replication slots, for the whole server, the
	// ones holding the most WAL first. A consumer that stops leaves
	// Replicas but stays here, still holding WAL.
	Slots []DatabaseReplicationSlot `json:"slots,omitempty"`
}

// DatabaseReplica is one replica as its primary sees it.
type DatabaseReplica struct {
	Name       string   `json:"name"`
	LagSeconds *float64 `json:"lag_seconds,omitempty"`
	State      string   `json:"state,omitempty"`
}

// DatabaseReplicationSlot is one replication slot and the WAL it holds.
type DatabaseReplicationSlot struct {
	Name string `json:"name"`
	// Type is logical or physical.
	Type string `json:"type"`
	// Plugin is a logical slot's decoding plugin. Database is the database
	// a logical slot reads.
	Plugin   string `json:"plugin,omitempty"`
	Database string `json:"database,omitempty"`
	// Active is whether a consumer is connected.
	Active bool `json:"active"`
	// WALStatus is the system's word for the WAL the slot needs: for
	// Postgres reserved, extended, unreserved or lost. Lost means the slot
	// can no longer serve its consumer.
	WALStatus string `json:"wal_status,omitempty"`
	// RetainedBytes is the WAL the slot keeps on disk, from its restart
	// position to the server's current position.
	RetainedBytes *int64 `json:"retained_bytes,omitempty"`
	// BehindBytes is the WAL the consumer has not confirmed, from its
	// confirmed position to the server's current position. A physical slot
	// has no confirmed position and leaves it out.
	BehindBytes *int64 `json:"behind_bytes,omitempty"`
	// SafeWALBytes is the WAL the server can write before the slot is at
	// risk of losing what it needs. Absent when the server sets no limit.
	SafeWALBytes *int64 `json:"safe_wal_bytes,omitempty"`
	// ConsumerApplicationName and ConsumerUser name the connection that
	// holds the slot, such as "Debezium Streaming" and "debezium", so a
	// receiver can tell which tool holds the WAL. Both are absent when no
	// consumer is connected, or when it isn't a replication connection.
	ConsumerApplicationName string `json:"consumer_application_name,omitempty"`
	ConsumerUser            string `json:"consumer_user,omitempty"`
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
