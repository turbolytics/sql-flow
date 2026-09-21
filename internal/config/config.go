package config

import (
	"math"

	"github.com/turbolytics/sql-flow/internal/errs"
)

type ErrorPolicy string

const (
	PolicyRaise  ErrorPolicy = "RAISE"
	PolicyIgnore ErrorPolicy = "IGNORE"
)

// Error
type Error struct {
	Policy ErrorPolicy `yaml:"policy"`
}

// SinkFormat
//
// Closed value sets carry jsonschema enum tags. Go has no enum type, so
// without them the generated schema would accept any string where the
// hand-written one accepted one of a few, and validation would get weaker as
// a side effect of generating it.
type SinkFormat struct {
	Type string `yaml:"type" jsonschema:"enum=parquet"`
}

// Various Sink Configs
type IcebergSink struct {
	CatalogName string `yaml:"catalog_name"`
	TableName   string `yaml:"table_name"`
}

// KafkaSSL configures TLS material for a Kafka connection.
type KafkaSSL struct {
	CALocation                      string `yaml:"ca_location,omitempty"`
	CertificateLocation             string `yaml:"certificate_location,omitempty"`
	KeyLocation                     string `yaml:"key_location,omitempty"`
	KeyPassword                     string `yaml:"key_password,omitempty"`
	EndpointIdentificationAlgorithm string `yaml:"endpoint_identification_algorithm,omitempty"`
}

// KafkaSASL configures SASL authentication for a Kafka connection.
type KafkaSASL struct {
	Mechanism string `yaml:"mechanism" jsonschema:"enum=PLAIN,enum=SCRAM-SHA-256,enum=SCRAM-SHA-512,enum=GSSAPI"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

type KafkaSink struct {
	// List of Kafka brokers.
	Brokers []string `yaml:"brokers"`
	// Target Kafka topic.
	Topic            string     `yaml:"topic"`
	SecurityProtocol string     `yaml:"security_protocol,omitempty"`
	SSL              *KafkaSSL  `yaml:"ssl,omitempty"`
	SASL             *KafkaSASL `yaml:"sasl,omitempty"`
}

type ConsoleSink struct{}

type SQLCommandSubstitution struct {
	Var  string `yaml:"var"`
	Type string `yaml:"type" jsonschema:"enum=uuid4"`
}

type SQLCommandSink struct {
	SQL           string                   `yaml:"sql"`
	Substitutions []SQLCommandSubstitution `yaml:"substitutions,omitempty"`
}

type ClickhouseSink struct {
	DSN   string `yaml:"dsn"`
	Table string `yaml:"table"`
}

// PostgresSink writes result rows to a Postgres table over a native client,
// at the cost of the batch: a COPY into a staging table and a server-side
// merge, in one transaction per batch.
//
// A batch is one transaction, so one value Postgres refuses, a NUL byte in
// text or a number too wide for its column, fails the whole batch with exit
// 10, and a source that replays on restart replays it. Clean such values in
// the handler's SQL.
type PostgresSink struct {
	// A libpq connection URI or key-value string. Connect directly, or through
	// a pooler in session mode: the staging table lives for the session, and
	// transaction pooling hands each transaction whichever server connection
	// is free.
	DSN string `yaml:"dsn"`
	// The target table, optionally schema-qualified. The sink never creates
	// it.
	Table string `yaml:"table"`
	// upsert replaces the row a key already identifies; append inserts every
	// row. Required: the two are different promises to the reader.
	Mode string `yaml:"mode" jsonschema:"enum=upsert,enum=append"`
	// The columns a row is identified by. Required for upsert, refused for
	// append. A unique index or constraint must cover exactly these columns.
	Key []string `yaml:"key,omitempty"`
}

// Sink is where result rows go. One block per destination, and the type field
// selects which one the pipeline builds.
//
// The same shape serves three places: a pipeline's sink, the dead-letter queue
// a failed record diverts to, and the sink a managed window collects into.
type Sink struct {
	// Sink identifier.
	Type string `yaml:"type"`
	// Format settings (e.g., for Parquet).
	Format *SinkFormat `yaml:"format,omitempty"`
	// Kafka-specific sink configuration.
	Kafka *KafkaSink `yaml:"kafka,omitempty"`
	// Console output sink configuration.
	Console *ConsoleSink `yaml:"console,omitempty"`
	// SQL-command sink configuration.
	SQLCommand *SQLCommandSink `yaml:"sqlcommand,omitempty"`
	// Iceberg-specific sink configuration.
	Iceberg *IcebergSink `yaml:"iceberg,omitempty"`
	// ClickHouse-specific sink configuration.
	Clickhouse *ClickhouseSink `yaml:"clickhouse,omitempty"`
	// Postgres-specific sink configuration.
	Postgres *PostgresSink `yaml:"postgres,omitempty"`
	// Bounds how long this sink keeps trying a destination that is not
	// answering. Omit to accept the defaults; set max_attempts to 1 to turn
	// retrying off. The kafka sink ignores this: franz-go already retries a
	// produce with its own backoff.
	Retry *SinkRetry `yaml:"retry,omitempty"`
}

// SinkRetry bounds how long a sink keeps trying a destination that is not
// answering. Omit the block to accept the defaults; set max_attempts to 1 to
// turn retrying off.
//
// The Kafka sink ignores this. franz-go already retries a produce with its own
// backoff, and a second ladder on top of that one is worse than none.
type SinkRetry struct {
	// Total attempts, including the first. 1 disables retrying.
	MaxAttempts int `yaml:"max_attempts,omitempty"`
	// Wait before the first retry. Doubles each attempt.
	InitialBackoffMS int `yaml:"initial_backoff_ms,omitempty"`
	// Ceiling on the backoff.
	MaxBackoffMS int `yaml:"max_backoff_ms,omitempty"`
	// Bounds the whole ladder. The postgres sink also bounds each attempt by
	// it, so raise it for a batch whose write takes longer. Keep it below
	// pipeline.flush_interval_seconds: the retry runs inside the open state
	// transaction, whose clock the window depends on.
	DeadlineSeconds int `yaml:"deadline_seconds,omitempty"`
}

// Window is a tumbling window the engine closes for the user. The handler
// writes rows keyed by a bucket start into the table; the engine keeps an
// event-time watermark, publishes every bucket the watermark has passed to
// the window's sink, and deletes it. Nothing here is SQL the user has to get
// right twice.
type Window struct {
	// The column holding each row's bucket start. TIMESTAMPTZ.
	TimeColumn string `yaml:"time_column"`
	// The bucket length. A bucket ends at time_column + size.
	SizeSeconds int `yaml:"size_seconds" jsonschema:"minimum=1"`
	// How far past a bucket's end the stream's own clock must reach before
	// the bucket closes. Absent means 0.
	GraceSeconds int `yaml:"grace_seconds,omitempty" jsonschema:"minimum=0"`
	// How long the stream may be quiet before every open bucket closes.
	// Absent means never: a stream that stops leaves its last bucket open.
	IdleCloseSeconds int `yaml:"idle_close_seconds,omitempty" jsonschema:"minimum=0"`
	// What happens to a row for a bucket that already closed. drop discards
	// it and counts it. reemit publishes emit_sql over the late rows alone,
	// for a sink that adds them to the bucket it holds; a sink that replaces
	// the bucket's value loses the rows published before.
	// Required: the two are different promises to the sink.
	LateRows string `yaml:"late_rows" jsonschema:"enum=drop,enum=reemit"`
	// How often the engine looks for closed buckets. Absent means 10.
	PollIntervalSecs int `yaml:"poll_interval_seconds,omitempty" jsonschema:"minimum=1"`
	// Shapes the closed rows before the sink. It reads one relation, closed,
	// which holds every row of every bucket that just closed. Absent means
	// SELECT * FROM closed.
	EmitSQL string `yaml:"emit_sql,omitempty"`
	// Where closed windows go.
	Sink Sink `yaml:"sink"`
}

// ReemitOverwrites reports the one pairing of late_rows and sink no pipeline
// may run: a postgres sink that upserts by key, handed a reemit. A reemit
// publishes emit_sql over the late rows alone, because the bucket's other
// rows were deleted when it closed, and the upsert replaces the bucket's
// published row with that. validate refuses it, and so does run, for a config
// that never went through validate.
func (w Window) ReemitOverwrites() bool {
	return w.LateRows == "reemit" && w.Sink.Type == "postgres" &&
		w.Sink.Postgres != nil && w.Sink.Postgres.Mode == "upsert"
}

// ReemitOverwritesMessage says why, for validate and run alike.
const ReemitOverwritesMessage = "late_rows is reemit and the postgres sink upserts. A reemit publishes " +
	"emit_sql over the late rows alone, and the sink replaces the bucket's row with that. Use drop"

// SQL Tables
type TableSQL struct {
	Name string `yaml:"name"`
	SQL  string `yaml:"sql"`
	// A tumbling window over this table, closed by the engine.
	Window *Window `yaml:"window,omitempty"`
}

// Tables holds the SQL tables the pipeline creates before it consumes.
type Tables struct {
	// List of tables with their SQL definitions and management configurations.
	SQL []TableSQL `yaml:"sql"`
}

// UDF registers a user-defined function the handler SQL can call.
type UDF struct {
	FunctionName string `yaml:"function_name"`
	ImportPath   string `yaml:"import_path"`
}

// SQLCommand is one statement run at startup, before any table is created.
type SQLCommand struct {
	// Name of the command for reference.
	Name string `yaml:"name"`
	// SQL statements to execute.
	SQL string `yaml:"sql"`
}

// Source Types
type KafkaSource struct {
	Brokers         []string `yaml:"brokers"`
	GroupID         string   `yaml:"group_id"`
	AutoOffsetReset string   `yaml:"auto_offset_reset" jsonschema:"enum=earliest,enum=latest"`
	Topics          []string `yaml:"topics"`

	SecurityProtocol string     `yaml:"security_protocol,omitempty" jsonschema:"enum=SASL_SSL,enum=SSL,enum=SASL_PLAINTEXT,enum=PLAINTEXT"`
	SSL              *KafkaSSL  `yaml:"ssl,omitempty"`
	SASL             *KafkaSASL `yaml:"sasl,omitempty"`

	// Bounds how far the consumer reads ahead of the pipeline. Omit the
	// block to accept the defaults, which bound a backlog replay to a few
	// fetches rather than the backlog.
	Fetch *KafkaFetch `yaml:"fetch,omitempty"`
}

// Defaults for KafkaFetch.
//
// The prefetch default is measured: the smallest depth within 5% of unbounded
// throughput on a 3M message backlog. See
// docs/superpowers/specs/2026-09-10-kafka-fetch-bound-design.md.
//
// The two byte values were 100 MiB and 10 MiB, which were never chosen -- they
// were what the source happened to set before the fetch block existed, kept as
// defaults so #252 changed only prefetch. They cost far more than they were
// worth. Read-ahead buffers, not the pipeline, were most of a log pipeline's
// memory. Measured on dev/config/examples/logs.rollup.clickhouse.yml over 3.46M
// records, in-container cgroup peak, output byte-identical in every row:
//
//	max_bytes / max_partition_bytes / prefetch	peak	throughput
//	100 MiB / 10 MiB / 2  (the old defaults)	985 MiB	201k/s
//	 16 MiB /  4 MiB / 2                    	460 MiB	207k/s
//	  4 MiB /  1 MiB / 2  (these)           	193 MiB	206k/s
//	  4 MiB /  1 MiB / 1                    	185 MiB	214k/s
//
// prefetch stays 2 because the sweep above measured it on a different
// workload and found 2 fastest there; only the byte bounds move.
const (
	DefaultKafkaFetchMaxBytes          = 4 << 20
	DefaultKafkaFetchMaxPartitionBytes = 1 << 20
	DefaultKafkaFetchPrefetch          = 2
)

// KafkaFetch bounds the consumer's read-ahead. The source holds at most
// prefetch fetches in the channel the pipeline reads from, plus the one it is
// waiting to send, and franz-go holds one more per broker. In bytes of
// payload, the worst case is (prefetch + 2) x brokers x max_bytes and the
// typical case is (prefetch + 2) x partitions x max_partition_bytes.
type KafkaFetch struct {
	// Bytes one fetch may return per broker. Kafka's fetch.max.bytes.
	MaxBytes int `yaml:"max_bytes,omitempty" jsonschema:"minimum=1"`
	// Bytes one fetch may return per partition. Kafka's max.partition.fetch.bytes.
	MaxPartitionBytes int `yaml:"max_partition_bytes,omitempty" jsonschema:"minimum=1"`
	// Fetches held ahead of the pipeline.
	Prefetch int `yaml:"prefetch,omitempty" jsonschema:"minimum=1"`
}

// Resolved fills absent fields with their defaults and checks the bounds.
// A nil receiver is the absent block.
func (f *KafkaFetch) Resolved() (KafkaFetch, error) {
	out := KafkaFetch{
		MaxBytes:          DefaultKafkaFetchMaxBytes,
		MaxPartitionBytes: DefaultKafkaFetchMaxPartitionBytes,
		Prefetch:          DefaultKafkaFetchPrefetch,
	}
	if f == nil {
		return out, nil
	}
	if f.MaxBytes != 0 {
		out.MaxBytes = f.MaxBytes
	}
	if f.MaxPartitionBytes != 0 {
		out.MaxPartitionBytes = f.MaxPartitionBytes
	}
	if f.Prefetch != 0 {
		out.Prefetch = f.Prefetch
	}

	if out.MaxBytes < 1 || out.MaxBytes > math.MaxInt32 {
		return out, errs.New(errs.CodeSourceInvalid, "kafka source: fetch.max_bytes must be between 1 and %d, got %d", math.MaxInt32, out.MaxBytes)
	}
	if out.MaxPartitionBytes < 1 || out.MaxPartitionBytes > math.MaxInt32 {
		return out, errs.New(errs.CodeSourceInvalid, "kafka source: fetch.max_partition_bytes must be between 1 and %d, got %d", math.MaxInt32, out.MaxPartitionBytes)
	}
	if out.MaxPartitionBytes > out.MaxBytes {
		return out, errs.New(errs.CodeSourceInvalid, "kafka source: fetch.max_partition_bytes (%d) must not exceed fetch.max_bytes (%d)", out.MaxPartitionBytes, out.MaxBytes)
	}
	if out.Prefetch < 1 {
		return out, errs.New(errs.CodeSourceInvalid, "kafka source: fetch.prefetch must be at least 1, got %d", out.Prefetch)
	}
	return out, nil
}

type WebsocketSource struct {
	URI string `yaml:"uri"`
}

// WebhookHMAC configures signature validation of incoming webhook bodies.
// SigKey names the digest the signature header is prefixed with, e.g.
// "sha256" for GitHub's "X-Hub-Signature-256: sha256=<hex>".
type WebhookHMAC struct {
	Header string `yaml:"header"`
	SigKey string `yaml:"sig_key"`
	Secret string `yaml:"secret"`
}

// DefaultWebhookMaxBodyBytes bounds one delivery. GitHub caps a payload at
// 25 MB, the largest of the senders the examples point at.
const DefaultWebhookMaxBodyBytes = 25 << 20

// DefaultWebhookMaxConnections bounds open connections to the listener. The
// pipeline takes one delivery at a time, so the slots only hold bodies; 64
// keeps the worst case at 64 x the body bound.
const DefaultWebhookMaxConnections = 64

// DefaultWebhookAddr is where the Python engine listened. Configs and reverse
// proxies written for it point at this port.
const DefaultWebhookAddr = "0.0.0.0:8001"

type WebhookSource struct {
	// The address the listener binds, as host:port. Defaults to
	// 0.0.0.0:8001. A platform that assigns the port sets it from the
	// environment: "0.0.0.0:{{ PORT }}".
	Addr          string       `yaml:"addr,omitempty"`
	SignatureType string       `yaml:"signature_type,omitempty" jsonschema:"enum=hmac"`
	HMAC          *WebhookHMAC `yaml:"hmac,omitempty"`
	// Bytes one request body may carry. A larger body is refused with 413
	// before it is read or its signature checked.
	MaxBodyBytes int64 `yaml:"max_body_bytes,omitempty" jsonschema:"minimum=1"`
	// Open connections the listener holds. Past it a new connection waits
	// in the backlog until one closes.
	MaxConnections int `yaml:"max_connections,omitempty" jsonschema:"minimum=1"`
}

// ResolvedMaxConnections is the connection bound in effect, defaulted. A nil
// receiver is the absent block.
func (w *WebhookSource) ResolvedMaxConnections() (int, error) {
	if w == nil || w.MaxConnections == 0 {
		return DefaultWebhookMaxConnections, nil
	}
	if w.MaxConnections < 1 {
		return 0, errs.New(errs.CodeSourceInvalid, "webhook source: max_connections must be at least 1, got %d", w.MaxConnections)
	}
	return w.MaxConnections, nil
}

// ResolvedMaxBodyBytes is the body bound in effect, defaulted. A nil receiver
// is the absent block.
func (w *WebhookSource) ResolvedMaxBodyBytes() (int64, error) {
	if w == nil || w.MaxBodyBytes == 0 {
		return DefaultWebhookMaxBodyBytes, nil
	}
	if w.MaxBodyBytes < 1 {
		return 0, errs.New(errs.CodeSourceInvalid, "webhook source: max_body_bytes must be at least 1, got %d", w.MaxBodyBytes)
	}
	return w.MaxBodyBytes, nil
}

// ResolvedAddr is the listen address in effect, defaulted. A nil receiver is
// the absent block. Empty means the default rather than an error: it is what
// a template renders for an unset variable, and net.Listen would read it as
// a port the kernel picks.
func (w *WebhookSource) ResolvedAddr() (string, error) {
	if w == nil || w.Addr == "" {
		return DefaultWebhookAddr, nil
	}
	if !validAddr(w.Addr) {
		return "", errs.New(errs.CodeSourceInvalid, "webhook source: addr %q is not a host:port", w.Addr)
	}
	return w.Addr, nil
}

// Source
type Source struct {
	Type      string           `yaml:"type"`
	Kafka     *KafkaSource     `yaml:"kafka,omitempty"`
	Websocket *WebsocketSource `yaml:"websocket,omitempty"`
	Webhook   *WebhookSource   `yaml:"webhook,omitempty"`
	Error     *Error           `yaml:"error,omitempty"`
}

// Handler
type Handler struct {
	Type               string `yaml:"type"`
	SQL                string `yaml:"sql"`
	SQLResultsCacheDir string `yaml:"sql_results_cache_dir,omitempty"`
	Table              string `yaml:"table,omitempty"`
}

// OnError configures what happens to a message or batch that fails.
// The dlq block is a full sink definition, used when policy is DLQ.
type OnError struct {
	// Defines how errors should be handled.
	Policy string `yaml:"policy" jsonschema:"enum=RAISE,enum=IGNORE,enum=DLQ"`
	// Dead-letter queue configuration. Failed messages will be routed to this
	// sink.
	DLQ *Sink `yaml:"dlq,omitempty"`
}

// StateConf points the pipeline's DuckDB at a file, so tables the handler
// writes -- window state above all -- survive a restart. Offsets are stored in
// the same database, which is what makes state and offsets recoverable
// together.
type StateConf struct {
	// File backing the pipeline's DuckDB database. Window state and Kafka
	// offsets are stored here and committed together.
	Path string `yaml:"path"`
}

// Pipeline is the source, the query, and the destination.
type Pipeline struct {
	// Name of the pipeline.
	Name string `yaml:"name,omitempty"`
	// Description of the pipeline.
	Description string `yaml:"description,omitempty"`
	// Configuration for the data source.
	Source  Source  `yaml:"source"`
	Handler Handler `yaml:"handler"`
	Sink    Sink    `yaml:"sink"`
	// Messages accumulated before the handler runs. Larger batches trade
	// latency for throughput.
	BatchSize int `yaml:"batch_size,omitempty"`
	// Longest a partial batch waits before it is invoked anyway.
	FlushIntervalSeconds int `yaml:"flush_interval_seconds,omitempty"`
	// Longest a shutdown may take after SIGTERM. The final batch, the
	// managers' final poll and the state syncs share it. Absent means 30.
	// When it passes the process exits 15, and the next start replays what
	// was not written.
	DrainDeadlineSeconds int `yaml:"drain_deadline_seconds,omitempty" jsonschema:"minimum=1"`
	// Where the pipeline keeps its DuckDB state. Absent means in-memory, and
	// state is lost on a crash.
	State *StateConf `yaml:"state,omitempty"`
	// Global error handling strategy for the pipeline.
	OnError *OnError `yaml:"on_error,omitempty"`
	// Where this instance reports itself. Absent means it reports nowhere,
	// which is the ordinary case for a pipeline with no control plane.
	TurboStats *TurboStats `yaml:"turbostats,omitempty"`
}

// Conf is a whole pipeline file.
type Conf struct {
	// Main pipeline configuration.
	Pipeline Pipeline `yaml:"pipeline"`
	// Predefined SQL tables used in the pipeline.
	Tables *Tables `yaml:"tables,omitempty"`
	// List of User-Defined Functions (UDFs) to be used in SQL queries.
	UDFs []UDF `yaml:"udfs,omitempty"`
	// List of SQL commands to execute before processing the pipeline.
	Commands []SQLCommand `yaml:"commands,omitempty"`
}
