package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/zeebo/assert"
)

func newTestADBCConn(t *testing.T) (adbc.Connection, func()) {
	t.Helper()

	lib := os.Getenv("SQLFLOW_DUCKDB_LIB")
	if lib == "" {
		lib = "/opt/homebrew/lib/libduckdb.dylib"
	}
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":     lib,
		"entrypoint": "duckdb_adbc_init",
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := db.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	return conn, func() { conn.Close() }
}

// TestDevInvoke_InferredMemBatch mirrors the Python suite's basic.agg.mem
// invoke case: two messages in, one aggregated row per city out.
func TestDevInvoke_InferredMemBatch(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	var out bytes.Buffer
	table, err := devInvoke(
		context.Background(),
		conn,
		"../../dev/config/examples/basic.agg.mem.yml",
		"../../dev/fixtures/basic.agg.jsonl",
		&out,
	)
	assert.NoError(t, err)
	defer table.Release()

	assert.Equal(t, int64(2), table.NumRows())
	assert.Equal(t, `{"city":"New York","city_count":1}
{"city":"Baltimore","city_count":1}
`, out.String())
}

// TestDevInvoke_BlueskyFirehose runs a shipped bluesky config end to end.
// Every firehose record carries JSON arrays -- langs, facets, embed.images --
// and none of the other invoke tests has an array anywhere in its fixture,
// which is how "unsupported json array value" reached a release.
func TestDevInvoke_BlueskyFirehose(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	var out bytes.Buffer
	table, err := devInvoke(
		context.Background(),
		conn,
		"../../dev/config/examples/bluesky/bluesky.raw.stdout.yml",
		"../../dev/fixtures/bluesky.jsonl",
		&out,
	)
	assert.NoError(t, err)
	defer table.Release()

	assert.Equal(t, int64(4), table.NumRows())

	// The arrays survive as lists, nested arbitrarily deep, rather than
	// collapsing to a string or failing the batch.
	rows := out.String()
	assert.That(t, strings.Contains(rows, `"langs":["en","ja"]`))
	assert.That(t, strings.Contains(rows, `"tag":"alpha"`))
	assert.That(t, strings.Contains(rows, `"aspectRatio":{"height":1068,"width":1068}`))
	// A message with no langs of its own yields an empty list, not a failure.
	assert.That(t, strings.Contains(rows, `"langs":[]`))

	// "reply" appears only in the last message. It is a column at all only
	// because struct fields are unioned across the batch; taking them from
	// the first message alone drops it silently.
	assert.That(t, strings.Contains(rows, `"reply"`))

	// JSON escapes arrive decoded. A \uXXXX that was never decoded would
	// render as a literal backslash-u here, and an undecoded newline as two
	// backslashes -- both are what corrupted rows looked like.
	assert.That(t, strings.Contains(rows, "café"))
	assert.That(t, !strings.Contains(rows, `\u00e9`))
	assert.That(t, !strings.Contains(rows, `\\n`))
}

// TestDevInvoke_StructuredBatch covers the path where the handler's table is
// created by a config command, which must run before the handler is built.
func TestDevInvoke_StructuredBatch(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	var out bytes.Buffer
	table, err := devInvoke(
		context.Background(),
		conn,
		"../../dev/config/examples/kafka.structured.mem.yml",
		"../../dev/fixtures/basic.agg.jsonl",
		&out,
	)
	assert.NoError(t, err)
	defer table.Release()

	assert.Equal(t, int64(2), table.NumRows())
	assert.Equal(t, `{"city":"New York","city_count":1}
{"city":"Baltimore","city_count":1}
`, out.String())
}

// TestDevInvoke_SkipsBlankLines matches the Python invoke, which strips each
// fixture line and writes only the non-empty ones.
func TestDevInvoke_SkipsBlankLines(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	fixture := filepath.Join(t.TempDir(), "fixture.jsonl")
	assert.NoError(t, os.WriteFile(fixture, []byte(
		"\n"+
			`{"event":"search","properties":{"city":"New York"},"user":{"id":"1"}}`+"\n"+
			"   \n"+
			`{"event":"search","properties":{"city":"New York"},"user":{"id":"2"}}`+"\n"+
			"\n",
	), 0o644))

	var out bytes.Buffer
	table, err := devInvoke(
		context.Background(),
		conn,
		"../../dev/config/examples/basic.agg.mem.yml",
		fixture,
		&out,
	)
	assert.NoError(t, err)
	defer table.Release()

	assert.Equal(t, int64(1), table.NumRows())
	assert.Equal(t, `{"city":"New York","city_count":2}`+"\n", out.String())
}

// TestDevInvoke_LargeFixture is the README quickstart case. It covers fixtures
// bigger than the read buffer, where the scanner refills and so overwrites the
// bytes of lines already handed to the handler; a handler keeps those slices
// until invoke, so any that were not copied out decode as garbage.
func TestDevInvoke_LargeFixture(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	var out bytes.Buffer
	table, err := devInvoke(
		context.Background(),
		conn,
		"../../dev/config/examples/basic.agg.mem.yml",
		"../../dev/fixtures/simple.json",
		&out,
	)
	assert.NoError(t, err)
	defer table.Release()

	assert.Equal(t, int64(2), table.NumRows())
	assert.Equal(t, `{"city":"New York","city_count":28672}
{"city":"Baltimore","city_count":28672}
`, out.String())
}

func TestDevInvoke_ReportsMissingFixture(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	var out bytes.Buffer
	_, err := devInvoke(
		context.Background(),
		conn,
		"../../dev/config/examples/basic.agg.mem.yml",
		filepath.Join(t.TempDir(), "missing.jsonl"),
		&out,
	)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "fixture"))
}

// An empty fixture is the degenerate unit test, and `dev invoke` is where
// users meet it first. It must report no rows rather than crash on the nil
// table an empty batch produces.
func TestDevInvoke_EmptyFixture(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	fixture := filepath.Join(t.TempDir(), "empty.jsonl")
	assert.NoError(t, os.WriteFile(fixture, nil, 0o644))

	var out bytes.Buffer
	table, err := devInvoke(
		context.Background(),
		conn,
		"../../dev/config/examples/basic.agg.mem.yml",
		fixture,
		&out,
	)
	assert.NoError(t, err)
	assert.Nil(t, table)
	assert.Equal(t, "", out.String())
}

// A fixture of nothing but blank lines reaches the handler with no messages
// by the same route.
func TestDevInvoke_FixtureOfOnlyBlankLines(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	fixture := filepath.Join(t.TempDir(), "blank.jsonl")
	assert.NoError(t, os.WriteFile(fixture, []byte("\n\n   \n"), 0o644))

	var out bytes.Buffer
	table, err := devInvoke(context.Background(), conn,
		"../../dev/config/examples/basic.agg.mem.yml", fixture, &out)
	assert.NoError(t, err)
	assert.Nil(t, table)
	assert.Equal(t, "", out.String())
}
