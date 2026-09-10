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
	// Bounds the whole ladder, not one attempt. Keep it below
	// pipeline.flush_interval_seconds: the retry runs inside the open state
	// transaction, whose clock the window depends on.
	DeadlineSeconds int `yaml:"deadline_seconds,omitempty"`
}

// Tumbling Window Manager
type TumblingWindow struct {
	CollectSQL string `yaml:"collect_closed_windows_sql"`
	DeleteSQL  string `yaml:"delete_closed_windows_sql"`
	// How often to collect closed windows. Optional; the manager applies its
	// own default when this is absent or not positive.
	PollIntervalSecs int `yaml:"poll_interval_seconds,omitempty"`
}

// Table Manager
type TableManager struct {
	TumblingWindow *TumblingWindow `yaml:"tumbling_window"`
	Sink           Sink            `yaml:"sink"`
}

// SQL Tables
type TableSQL struct {
	Name    string        `yaml:"name"`
	SQL     string        `yaml:"sql"`
	Manager *TableManager `yaml:"manager,omitempty"`
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

// Defaults for KafkaFetch. The two byte values are what the source set
// before the block existed. The prefetch default is measured: the smallest
// depth within 5% of unbounded throughput on a 3M message backlog. See
// docs/superpowers/specs/2026-09-10-kafka-fetch-bound-design.md.
const (
	DefaultKafkaFetchMaxBytes          = 100 << 20
	DefaultKafkaFetchMaxPartitionBytes = 10 << 20
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

type WebhookSource struct {
	SignatureType string       `yaml:"signature_type,omitempty" jsonschema:"enum=hmac"`
	HMAC          *WebhookHMAC `yaml:"hmac,omitempty"`
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
	// Where the pipeline keeps its DuckDB state. Absent means in-memory, and
	// state is lost on a crash.
	State *StateConf `yaml:"state,omitempty"`
	// Global error handling strategy for the pipeline.
	OnError *OnError `yaml:"on_error,omitempty"`
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
