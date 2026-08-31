package sinks

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	icebergtable "github.com/apache/iceberg-go/table"
	"gopkg.in/yaml.v3"

	_ "modernc.org/sqlite" // sqlite driver for SQL-backed iceberg catalogs
)

// icebergAppendBatchSize bounds the record size handed to the writer when a
// batch is split into data files.
const icebergAppendBatchSize = 1 << 20

// IcebergSink appends result batches to an Iceberg table.
type IcebergSink struct {
	table *icebergtable.Table

	mu     sync.Mutex
	tables []arrow.Table
}

func NewIcebergSink(ctx context.Context, catalogName, tableName string) (*IcebergSink, error) {
	if catalogName == "" || tableName == "" {
		return nil, fmt.Errorf("iceberg sink: catalog_name and table_name are required")
	}

	props, err := icebergCatalogProperties(catalogName)
	if err != nil {
		return nil, err
	}

	if err := ensureIcebergTypeColumn(props); err != nil {
		return nil, err
	}

	cat, err := catalog.Load(ctx, catalogName, props)
	if err != nil {
		return nil, fmt.Errorf("iceberg sink: load catalog %q: %w", catalogName, err)
	}

	ident := catalog.ToIdentifier(strings.Split(tableName, ".")...)
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("iceberg sink: load table %q: %w", tableName, err)
	}

	return &IcebergSink{table: tbl}, nil
}

func (s *IcebergSink) WriteTable(batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch.Retain()
	s.tables = append(s.tables, batch)
	return nil
}

func (s *IcebergSink) Batch() (arrow.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.tables) == 0 {
		return nil, nil
	}
	return s.tables[len(s.tables)-1], nil
}

func (s *IcebergSink) Flush() error {
	s.mu.Lock()
	tables := s.tables
	s.tables = nil
	s.mu.Unlock()

	if len(tables) == 0 {
		return nil
	}
	defer func() {
		for _, t := range tables {
			t.Release()
		}
	}()

	ctx := context.Background()
	for _, t := range tables {
		if t.NumRows() == 0 {
			continue
		}
		updated, err := s.table.AppendTable(ctx, t, icebergAppendBatchSize, nil)
		if err != nil {
			return fmt.Errorf("iceberg sink: append: %w", err)
		}
		s.table = updated
	}
	return nil
}

// ensureIcebergTypeColumn reconciles a schema difference between the two
// Iceberg SQL catalog implementations: iceberg-go records whether each entry
// is a table or a view in an iceberg_type column and filters on it, while
// pyiceberg (through 0.11, the current release) never creates that column, so
// a catalog created by the Python engine is unreadable from Go.
//
// The column is added with a TABLE default, which is additive and leaves the
// catalog fully usable by pyiceberg: it names the columns it reads and writes,
// so an extra one is ignored.
func ensureIcebergTypeColumn(props iceberg.Properties) error {
	if props.Get(sqlcat.DriverKey, "") != "sqlite" {
		return nil
	}

	db, err := sql.Open("sqlite", props.Get("uri", ""))
	if err != nil {
		return fmt.Errorf("iceberg sink: open catalog: %w", err)
	}
	defer db.Close()

	var exists int
	err = db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='iceberg_tables'`,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("iceberg sink: inspect catalog: %w", err)
	}
	if exists == 0 {
		// A catalog turbine creates itself will have the column already.
		return nil
	}

	rows, err := db.Query(`SELECT name FROM pragma_table_info('iceberg_tables')`)
	if err != nil {
		return fmt.Errorf("iceberg sink: inspect catalog columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if name == "iceberg_type" {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if _, err := db.Exec(
		`ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR(5) DEFAULT 'TABLE'`,
	); err != nil {
		return fmt.Errorf("iceberg sink: add iceberg_type column: %w", err)
	}
	if _, err := db.Exec(
		`UPDATE iceberg_tables SET iceberg_type = 'TABLE' WHERE iceberg_type IS NULL`,
	); err != nil {
		return fmt.Errorf("iceberg sink: backfill iceberg_type: %w", err)
	}
	return nil
}

// pyicebergConfig is the subset of .pyiceberg.yaml turbine reads.
type pyicebergConfig struct {
	Catalog map[string]map[string]string `yaml:"catalog"`
}

// icebergCatalogProperties resolves a named catalog the way pyiceberg does,
// so a config written for the Python engine finds the same catalog: from
// .pyiceberg.yaml under PYICEBERG_HOME or the home directory, with
// PYICEBERG_CATALOG__<NAME>__<KEY> environment variables taking precedence.
func icebergCatalogProperties(name string) (iceberg.Properties, error) {
	props := iceberg.Properties{}

	fileProps, err := pyicebergFileProperties(name)
	if err != nil {
		return nil, err
	}
	for k, v := range fileProps {
		props[k] = v
	}

	envPrefix := "PYICEBERG_CATALOG__" + strings.ToUpper(name) + "__"
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 && strings.HasPrefix(parts[0], envPrefix) {
			key := strings.ToLower(strings.TrimPrefix(parts[0], envPrefix))
			props[strings.ReplaceAll(key, "__", ".")] = parts[1]
		}
	}

	uri := props.Get("uri", "")
	if uri == "" {
		return nil, fmt.Errorf("iceberg sink: no uri configured for catalog %q", name)
	}

	// Only SQL-backed catalogs are supported so far; a REST catalog would be
	// selected here by its uri scheme.
	scheme, rest, found := strings.Cut(uri, "://")
	if !found {
		return nil, fmt.Errorf("iceberg sink: malformed catalog uri %q", uri)
	}

	switch scheme {
	case "sqlite":
		props["type"] = "sql"
		props[sqlcat.DriverKey] = "sqlite"
		props[sqlcat.DialectKey] = string(sqlcat.SQLite)
		// pyiceberg writes sqlite:////abs/path; the Go driver wants the path.
		props["uri"] = "/" + strings.TrimLeft(rest, "/")
	default:
		return nil, fmt.Errorf("iceberg sink: unsupported catalog uri scheme %q", scheme)
	}

	if wh := props.Get("warehouse", ""); wh != "" {
		props["warehouse"] = strings.TrimPrefix(wh, "file://")
	}

	// iceberg-go's SQL registrar resolves the catalog's own name with
	// props.Get(name, "sql"), so without a property under this key the
	// catalog queries for rows named "sql" and finds nothing that the Python
	// engine wrote.
	props[name] = name

	return props, nil
}

func pyicebergFileProperties(name string) (map[string]string, error) {
	var candidates []string
	if home := os.Getenv("PYICEBERG_HOME"); home != "" {
		candidates = append(candidates, filepath.Join(home, ".pyiceberg.yaml"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".pyiceberg.yaml"))
	}

	for _, path := range candidates {
		raw, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}

		var conf pyicebergConfig
		if err := yaml.Unmarshal(raw, &conf); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", path, err)
		}
		if props, ok := conf.Catalog[name]; ok {
			return props, nil
		}
	}

	return map[string]string{}, nil
}
