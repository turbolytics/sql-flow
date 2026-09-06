package sinks

// Iceberg shipped for months with a unit test that skipped and printed "ok".
// Nothing here skips: a SQL catalog backed by sqlite and a warehouse in a temp
// directory are both local files, so the sink can be driven end to end without
// a catalog service, object store or network.

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/zeebo/assert"
)

// isolateCatalogLookup points catalog resolution at an empty directory, so a
// developer's own ~/.pyiceberg.yaml cannot decide what these tests see.
func isolateCatalogLookup(t *testing.T) {
	t.Helper()

	empty := t.TempDir()
	t.Setenv("PYICEBERG_HOME", empty)
	t.Setenv("HOME", empty)
}

// writePyicebergConfig writes a .pyiceberg.yaml under PYICEBERG_HOME, the file
// a config written for the Python engine resolves through.
func writePyicebergConfig(t *testing.T, body string) {
	t.Helper()

	home := t.TempDir()
	t.Setenv("PYICEBERG_HOME", home)
	t.Setenv("HOME", t.TempDir())

	path := filepath.Join(home, ".pyiceberg.yaml")
	assert.NoError(t, os.WriteFile(path, []byte(body), 0o644))
}

func TestSinkIceberg_NewRequiresCatalogAndTable(t *testing.T) {
	ctx := context.Background()

	_, err := NewIcebergSink(ctx, "", "default.events")
	assert.Error(t, err)

	_, err = NewIcebergSink(ctx, "warehouse", "")
	assert.Error(t, err)
}

// A config written for the Python engine names a catalog, not a connection.
// Resolving that name the way pyiceberg does is what lets the same config run
// on either engine, so the translation is asserted key by key.
func TestSinkIceberg_CatalogPropertiesFromPyicebergFile(t *testing.T) {
	writePyicebergConfig(t, `catalog:
  local:
    uri: sqlite:////tmp/wh/catalog.db
    warehouse: file:///tmp/wh
`)

	props, err := icebergCatalogProperties("local")
	assert.NoError(t, err)

	assert.Equal(t, "sql", props.Get("type", ""))
	assert.Equal(t, "sqlite", props.Get(sqlcat.DriverKey, ""))
	assert.Equal(t, string(sqlcat.SQLite), props.Get(sqlcat.DialectKey, ""))

	// pyiceberg writes sqlite:////abs/path; the Go driver takes the path.
	assert.Equal(t, "/tmp/wh/catalog.db", props.Get("uri", ""))

	// The Go writer takes a directory, not a file URL.
	assert.Equal(t, "/tmp/wh", props.Get("warehouse", ""))

	// iceberg-go's SQL registrar resolves the catalog's own name through
	// props.Get(name, "sql"). Without this key it queries for rows whose
	// catalog_name is "sql" and finds nothing the Python engine wrote.
	assert.Equal(t, "local", props.Get("local", ""))
}

// The release suite passes the catalog in the environment and never writes a
// file. Both forms must work, and the environment must win: it is how one
// image runs against a different catalog per deployment.
func TestSinkIceberg_CatalogPropertiesEnvOverridesFile(t *testing.T) {
	writePyicebergConfig(t, `catalog:
  local:
    uri: sqlite:////tmp/from-file/catalog.db
`)
	t.Setenv("PYICEBERG_CATALOG__LOCAL__URI", "sqlite:////tmp/from-env/catalog.db")
	t.Setenv("PYICEBERG_CATALOG__LOCAL__WAREHOUSE", "file:///tmp/from-env")

	props, err := icebergCatalogProperties("local")
	assert.NoError(t, err)

	assert.Equal(t, "/tmp/from-env/catalog.db", props.Get("uri", ""))
	assert.Equal(t, "/tmp/from-env", props.Get("warehouse", ""))
}

// A catalog that resolves to nothing must say so at startup. Reporting an
// empty property set instead would have iceberg-go fail later with a message
// about a missing driver, which names the wrong problem.
func TestSinkIceberg_CatalogPropertiesRequireAURI(t *testing.T) {
	isolateCatalogLookup(t)

	_, err := icebergCatalogProperties("nowhere")
	assert.Error(t, err)
}

// Only SQL-backed catalogs are supported so far. A REST catalog must be
// rejected by name rather than half-configured as a SQL one.
func TestSinkIceberg_CatalogPropertiesRejectAnUnsupportedScheme(t *testing.T) {
	isolateCatalogLookup(t)
	t.Setenv("PYICEBERG_CATALOG__REMOTE__URI", "https://catalog.example.com")

	_, err := icebergCatalogProperties("remote")
	assert.Error(t, err)
}

// pyiceberg, through 0.11, never creates the iceberg_type column that
// iceberg-go filters on. A catalog the Python engine wrote is therefore
// unreadable from Go until the column is added, and every table in it reads as
// missing -- the exact failure a user migrating between engines hits first.
func TestSinkIceberg_AddsPyicebergsMissingTypeColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")

	db, err := sql.Open("sqlite", path)
	assert.NoError(t, err)

	// pyiceberg's own DDL, which has no iceberg_type column.
	_, err = db.Exec(`CREATE TABLE iceberg_tables (
		catalog_name VARCHAR(255) NOT NULL,
		table_namespace VARCHAR(255) NOT NULL,
		table_name VARCHAR(255) NOT NULL,
		metadata_location VARCHAR(1000),
		previous_metadata_location VARCHAR(1000),
		PRIMARY KEY (catalog_name, table_namespace, table_name)
	)`)
	assert.NoError(t, err)
	_, err = db.Exec(`INSERT INTO iceberg_tables
		(catalog_name, table_namespace, table_name, metadata_location)
		VALUES ('local', 'default', 'events', 'file:///tmp/wh/meta.json')`)
	assert.NoError(t, err)
	assert.NoError(t, db.Close())

	props := iceberg.Properties{sqlcat.DriverKey: "sqlite", "uri": path}
	assert.NoError(t, ensureIcebergTypeColumn(props))

	db, err = sql.Open("sqlite", path)
	assert.NoError(t, err)
	defer db.Close()

	// The existing row must come back as a table. A NULL here and iceberg-go's
	// `WHERE iceberg_type = 'TABLE'` still hides it.
	var kind string
	assert.NoError(t, db.QueryRow(
		`SELECT iceberg_type FROM iceberg_tables WHERE table_name = 'events'`).Scan(&kind))
	assert.Equal(t, sqlcat.TableType, kind)

	// Running twice must not fail: every start calls this.
	assert.NoError(t, ensureIcebergTypeColumn(props))
}

// A catalog with no iceberg_tables table at all is one turbine is about to
// create itself, and creating it is iceberg-go's job, not this function's.
func TestSinkIceberg_TypeColumnSkipsAnEmptyCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")

	props := iceberg.Properties{sqlcat.DriverKey: "sqlite", "uri": path}
	assert.NoError(t, ensureIcebergTypeColumn(props))
}

// newLocalIcebergTable creates a sqlite-backed catalog and one table in a temp
// directory, and returns the catalog and table names the sink resolves them
// by. Everything it touches is a local file.
func newLocalIcebergTable(t *testing.T) (string, string) {
	t.Helper()

	isolateCatalogLookup(t)

	warehouse := t.TempDir()
	const catalogName = "sinktest"
	const tableName = "default.city_events"

	t.Setenv("PYICEBERG_CATALOG__SINKTEST__URI",
		"sqlite:///"+filepath.Join(warehouse, "catalog.db"))
	t.Setenv("PYICEBERG_CATALOG__SINKTEST__WAREHOUSE", "file://"+warehouse)

	ctx := context.Background()
	props, err := icebergCatalogProperties(catalogName)
	assert.NoError(t, err)

	cat, err := catalog.Load(ctx, catalogName, props)
	assert.NoError(t, err)

	assert.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("default"), nil))

	// Matches newTestTable: a string key and an integer count, both optional,
	// which is the shape an aggregating handler emits.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "city", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "count", Type: iceberg.PrimitiveTypes.Int64},
	)
	_, err = cat.CreateTable(ctx, catalog.ToIdentifier("default", "city_events"), schema)
	assert.NoError(t, err)

	return catalogName, tableName
}

// icebergRowCount reloads the table through the catalog and counts what is
// committed. Reading through the sink's own handle would only prove the sink
// agrees with itself.
func icebergRowCount(t *testing.T, catalogName, tableName string) int64 {
	t.Helper()

	ctx := context.Background()
	props, err := icebergCatalogProperties(catalogName)
	assert.NoError(t, err)

	cat, err := catalog.Load(ctx, catalogName, props)
	assert.NoError(t, err)

	tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier("default", "city_events"))
	assert.NoError(t, err)

	result, err := tbl.Scan().ToArrowTable(ctx)
	assert.NoError(t, err)
	defer result.Release()

	return result.NumRows()
}

// The sink's whole job, proven against a committed snapshot rather than
// against its own buffer.
func TestSinkIceberg_AppendsEveryRow(t *testing.T) {
	catalogName, tableName := newLocalIcebergTable(t)

	s, err := NewIcebergSink(context.Background(), catalogName, tableName)
	assert.NoError(t, err)

	first := newTestTable(t, []string{"nyc", "sfo"}, []int64{1, 2})
	defer first.Release()
	second := newTestTable(t, []string{"lhr"}, []int64{3})
	defer second.Release()

	ctx := context.Background()
	assert.NoError(t, s.WriteTable(ctx, first))
	assert.NoError(t, s.WriteTable(ctx, second))

	// Buffered, not written. The batch becomes durable on flush, which is what
	// lets the pipeline commit its offsets only after the flush returns.
	assert.Equal(t, int64(0), icebergRowCount(t, catalogName, tableName))

	assert.NoError(t, s.Flush(ctx))
	assert.Equal(t, int64(3), icebergRowCount(t, catalogName, tableName))
}

// A second flush with nothing buffered must append nothing. The pipeline
// flushes on an interval whether or not a batch arrived, so a sink that
// re-appended its last batch would duplicate the table once per interval.
func TestSinkIceberg_FlushWithNothingPendingAppendsNothing(t *testing.T) {
	catalogName, tableName := newLocalIcebergTable(t)

	s, err := NewIcebergSink(context.Background(), catalogName, tableName)
	assert.NoError(t, err)

	ctx := context.Background()
	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()

	assert.NoError(t, s.WriteTable(ctx, table))
	assert.NoError(t, s.Flush(ctx))
	assert.NoError(t, s.Flush(ctx))
	assert.NoError(t, s.Flush(ctx))

	assert.Equal(t, int64(1), icebergRowCount(t, catalogName, tableName))
}

// A handler whose query matched nothing yields an empty table. Appending it
// would commit a snapshot with no data files, so a table's history would fill
// with empty commits on a quiet topic.
func TestSinkIceberg_EmptyBatchAppendsNothing(t *testing.T) {
	catalogName, tableName := newLocalIcebergTable(t)

	s, err := NewIcebergSink(context.Background(), catalogName, tableName)
	assert.NoError(t, err)

	empty := array.NewTable(arrow.NewSchema(nil, nil), nil, 0)
	defer empty.Release()

	ctx := context.Background()
	assert.NoError(t, s.WriteTable(ctx, empty))
	assert.NoError(t, s.Flush(ctx))

	assert.Equal(t, int64(0), icebergRowCount(t, catalogName, tableName))
}

// Batch is what the tumbling-window manager reads back after a write.
func TestSinkIceberg_BatchIsTheLastWrite(t *testing.T) {
	catalogName, tableName := newLocalIcebergTable(t)

	s, err := NewIcebergSink(context.Background(), catalogName, tableName)
	assert.NoError(t, err)

	batch, err := s.Batch()
	assert.NoError(t, err)
	assert.Nil(t, batch)

	table := newTestTable(t, []string{"nyc", "sfo"}, []int64{1, 2})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	batch, err = s.Batch()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), batch.NumRows())
}

// A table the catalog does not have must fail the start. Discovering it on the
// first flush instead loses the batch and every offset behind it.
func TestSinkIceberg_MissingTableFailsTheStart(t *testing.T) {
	catalogName, _ := newLocalIcebergTable(t)

	_, err := NewIcebergSink(context.Background(), catalogName, "default.no_such_table")
	assert.Error(t, err)
}
