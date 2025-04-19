package config

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
type SinkFormat struct {
	Type string `yaml:"type"`
}

// Various Sink Configs
type IcebergSink struct {
	CatalogName string `yaml:"catalog_name"`
	TableName   string `yaml:"table_name"`
}

type KafkaSink struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

type ConsoleSink struct{}

type SQLCommandSubstitution struct {
	Var  string `yaml:"var"`
	Type string `yaml:"type"`
}

type SQLCommandSink struct {
	SQL           string                   `yaml:"sql"`
	Substitutions []SQLCommandSubstitution `yaml:"substitutions,omitempty"`
}

type ClickhouseSink struct {
	DSN   string `yaml:"dsn"`
	Table string `yaml:"table"`
}

// Unified Sink
type Sink struct {
	Type       string          `yaml:"type"`
	Format     *SinkFormat     `yaml:"format,omitempty"`
	Kafka      *KafkaSink      `yaml:"kafka,omitempty"`
	Console    *ConsoleSink    `yaml:"console,omitempty"`
	SQLCommand *SQLCommandSink `yaml:"sqlcommand,omitempty"`
	Iceberg    *IcebergSink    `yaml:"iceberg,omitempty"`
	Clickhouse *ClickhouseSink `yaml:"clickhouse,omitempty"`
}

// Tumbling Window Manager
type TumblingWindow struct {
	CollectSQL       string `yaml:"collect_closed_windows_sql"`
	DeleteSQL        string `yaml:"delete_closed_windows_sql"`
	PollIntervalSecs int    `yaml:"poll_interval_seconds"`
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

// Tables
type Tables struct {
	SQL []TableSQL `yaml:"sql"`
}

// UDFs
type UDF struct {
	FunctionName string `yaml:"function_name"`
	ImportPath   string `yaml:"import_path"`
}

// SQL Command
type SQLCommand struct {
	Name string `yaml:"name"`
	SQL  string `yaml:"sql"`
}

// Source Types
type KafkaSource struct {
	Brokers         []string `yaml:"brokers"`
	GroupID         string   `yaml:"group_id"`
	AutoOffsetReset string   `yaml:"auto_offset_reset"`
	Topics          []string `yaml:"topics"`
}

type WebsocketSource struct {
	URI string `yaml:"uri"`
}

// Source
type Source struct {
	Type      string           `yaml:"type"`
	Kafka     *KafkaSource     `yaml:"kafka,omitempty"`
	Websocket *WebsocketSource `yaml:"websocket,omitempty"`
	Error     *Error           `yaml:"error,omitempty"`
}

// Handler
type Handler struct {
	Type               string `yaml:"type"`
	SQL                string `yaml:"sql"`
	SQLResultsCacheDir string `yaml:"sql_results_cache_dir,omitempty"`
	Table              string `yaml:"table,omitempty"`
}

// Pipeline
type Pipeline struct {
	Source               Source  `yaml:"source"`
	Handler              Handler `yaml:"handler"`
	Sink                 Sink    `yaml:"sink"`
	BatchSize            int     `yaml:"batch_size,omitempty"`
	FlushIntervalSeconds int     `yaml:"flush_interval_seconds,omitempty"`
}

// Conf
type Conf struct {
	Pipeline Pipeline     `yaml:"pipeline"`
	Tables   *Tables      `yaml:"tables,omitempty"`
	UDFs     []UDF        `yaml:"udfs,omitempty"`
	Commands []SQLCommand `yaml:"commands,omitempty"`
}
