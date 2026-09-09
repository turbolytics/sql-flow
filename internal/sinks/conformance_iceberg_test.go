package sinks

// The Iceberg sink under the conformance harness.
//
// No container and no network: the catalog is sqlite and the warehouse is a
// directory, so the fault is a warehouse the sink cannot write into -- a full
// volume, a revoked credential, a read-only mount. The catalog lives outside
// the warehouse so making the warehouse unwritable does not also break the
// read-back; a broken destination must never be indistinguishable from an
// empty one.

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/zeebo/assert"
)

func TestSinkIceberg_Conformance(t *testing.T) {
	isolateCatalogLookup(t)

	// Two directories on purpose. Break makes the warehouse unwritable, and
	// the catalog must stay readable and writable for the read-back to work.
	warehouse := t.TempDir()
	catalogDir := t.TempDir()

	const catalogName = "conformance"
	t.Setenv("PYICEBERG_CATALOG__CONFORMANCE__URI",
		"sqlite:///"+filepath.Join(catalogDir, "catalog.db"))
	t.Setenv("PYICEBERG_CATALOG__CONFORMANCE__WAREHOUSE", "file://"+warehouse)

	ctx := context.Background()
	props, err := icebergCatalogProperties(catalogName)
	assert.NoError(t, err)

	cat, err := catalog.Load(ctx, catalogName, props)
	assert.NoError(t, err)
	assert.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("default"), nil))

	// The harness writes a single int64 "id" column.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	)
	_, err = cat.CreateTable(ctx, catalog.ToIdentifier("default", "rows"), schema)
	assert.NoError(t, err)

	conformance.Sinks(t, conformance.SinkSubject{
		Integration: "sink.iceberg",

		New: func(t *testing.T) core.Sink {
			s, err := NewIcebergSink(ctx, catalogName, "default.rows")
			assert.NoError(t, err)
			return s
		},

		Break: func(t *testing.T) { chmodTree(t, warehouse, 0o500) },
		Heal:  func(t *testing.T) { chmodTree(t, warehouse, 0o700) },

		ReadBack: func(t *testing.T) []conformance.Row {
			return icebergIDs(t, catalogName)
		},

		Table: func(t *testing.T, id int64) arrow.Table { return oneRowTable(t, id) },

		// OrderedReadBack stays false. A scan reads the table's data files, and
		// each append writes its own file with a generated name, so the rows
		// come back in file order rather than append order. Observed: a
		// read-back of [id=2, id=1] on roughly one run in six.
	})
}

// chmodTree sets the mode of every directory under root, and root itself.
// Only directories: 0500 on a directory blocks creating a file in it while
// leaving what is already there readable.
func chmodTree(t *testing.T, root string, mode fs.FileMode) {
	t.Helper()

	var dirs []string
	assert.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	}))

	// Deepest first when locking, so a parent turning read-only does not stop
	// the walk from reaching its children.
	for i := len(dirs) - 1; i >= 0; i-- {
		assert.NoError(t, os.Chmod(dirs[i], mode))
	}
	// The temp directory has to be writable again for t.TempDir cleanup.
	if mode&0o200 == 0 {
		t.Cleanup(func() { chmodTreeBestEffort(root, 0o700) })
	}
}

func chmodTreeBestEffort(root string, mode fs.FileMode) {
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			_ = os.Chmod(path, mode)
		}
		return nil
	})
}

// icebergIDs reloads the table through the catalog and reads every committed
// id. Reading through the sink's own handle would only prove the sink agrees
// with itself.
func icebergIDs(t *testing.T, catalogName string) []conformance.Row {
	t.Helper()
	ctx := context.Background()

	props, err := icebergCatalogProperties(catalogName)
	assert.NoError(t, err)

	cat, err := catalog.Load(ctx, catalogName, props)
	assert.NoError(t, err)

	tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier("default", "rows"))
	assert.NoError(t, err)

	result, err := tbl.Scan().ToArrowTable(ctx)
	assert.NoError(t, err)
	defer result.Release()

	var out []conformance.Row
	if result.NumRows() == 0 {
		return out
	}
	for _, chunk := range result.Column(0).Data().Chunks() {
		arr := chunk.(*array.Int64)
		for i := 0; i < arr.Len(); i++ {
			out = append(out, conformance.Row{"id": arr.Value(i)})
		}
	}
	return out
}
