package serve

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

func newConn(t *testing.T) adbc.Connection {
	t.Helper()
	conn, err := duckdb.Open(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func execSQL(t *testing.T, conn adbc.Connection, sql string) {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(sql))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
}

// encodeSQL runs sql and encodes its rows the way a request does.
func encodeSQL(t *testing.T, conn adbc.Connection, sql string, maxRows int) result {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(sql))
	rdr, _, err := stmt.ExecuteQuery(context.Background())
	assert.NoError(t, err)
	defer rdr.Release()

	res, err := readRows(rdr, maxRows)
	assert.NoError(t, err)
	return res
}

func decodeRows(t *testing.T, res result) []map[string]any {
	t.Helper()
	var rows []map[string]any
	assert.NoError(t, json.Unmarshal(res.Rows, &rows))
	return rows
}

// Each value rule, against what DuckDB actually hands over. A session zone
// other than UTC proves a zoned timestamp is rendered in UTC regardless.
func TestCliServe_EncodeRendersEachTypeByTheContract(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)
	execSQL(t, conn, "SET TimeZone='America/New_York'")

	res := encodeSQL(t, conn, `SELECT
		'en'::VARCHAR AS s,
		true AS b,
		42::BIGINT AS i,
		1.5::DOUBLE AS d,
		'nan'::DOUBLE AS nan,
		'inf'::DOUBLE AS inf,
		'-inf'::FLOAT AS neg_inf,
		1.1::FLOAT AS f,
		12.34::DECIMAL(10,2) AS dec,
		170141183460469231731687303715884105727::HUGEINT AS huge,
		TIMESTAMPTZ '2026-09-10 00:00:00+00' AS tz,
		TIMESTAMP '2026-09-10 01:02:03.456' AS naive,
		DATE '2026-09-10' AS day,
		[1, 2] AS list,
		{'a': 1} AS obj,
		NULL::VARCHAR AS missing`, 10)

	types := map[string]string{}
	for _, c := range res.Columns {
		types[c.Name] = c.Type
	}
	assert.Equal(t, "VARCHAR", types["s"])
	assert.Equal(t, "BOOLEAN", types["b"])
	assert.Equal(t, "BIGINT", types["i"])
	assert.Equal(t, "DOUBLE", types["d"])
	assert.Equal(t, "FLOAT", types["f"])
	assert.Equal(t, "DECIMAL(10,2)", types["dec"])
	assert.Equal(t, "DECIMAL(38,0)", types["huge"])
	assert.Equal(t, "TIMESTAMP WITH TIME ZONE", types["tz"])
	assert.Equal(t, "TIMESTAMP", types["naive"])
	assert.Equal(t, "DATE", types["day"])
	assert.Equal(t, "LIST", types["list"])
	assert.Equal(t, "STRUCT", types["obj"])

	assert.Equal(t, 1, res.RowCount)
	assert.False(t, res.Truncated)

	var rows []map[string]json.RawMessage
	assert.NoError(t, json.Unmarshal(res.Rows, &rows))
	row := rows[0]
	for name, want := range map[string]string{
		"s":       `"en"`,
		"b":       `true`,
		"i":       `42`,
		"d":       `1.5`,
		"nan":     `"NaN"`,
		"inf":     `"Infinity"`,
		"neg_inf": `"-Infinity"`,
		"f":       `1.1`,
		"dec":     `"12.34"`,
		"huge":    `"170141183460469231731687303715884105727"`,
		"tz":      `"2026-09-10T00:00:00Z"`,
		"naive":   `"2026-09-10T01:02:03.456"`,
		"day":     `"2026-09-10"`,
		"list":    `[1,2]`,
		"obj":     `{"a":1}`,
		"missing": `null`,
	} {
		assert.Equal(t, want, string(row[name]))
	}
}

// A DuckDB ENUM arrives as an Arrow dictionary. It reports the value type,
// and each row renders its own value, not its index.
func TestCliServe_EncodeRendersAnEnumByValue(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)
	execSQL(t, conn, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")

	res := encodeSQL(t, conn,
		"SELECT m::mood AS m FROM (VALUES ('happy'), ('sad')) AS t(m)", 10)

	assert.Equal(t, "VARCHAR", res.Columns[0].Type)
	rows := decodeRows(t, res)
	assert.Equal(t, "happy", rows[0]["m"])
	assert.Equal(t, "sad", rows[1]["m"])
}

// max_rows cuts the result and says so. Exactly max_rows rows is not a cut,
// and the check spans record batches: DuckDB hands over 2048 rows at a time.
func TestCliServe_EncodeTruncatesAtMaxRows(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)

	cut := encodeSQL(t, conn, "SELECT range AS n FROM range(5000)", 3000)
	assert.Equal(t, 3000, cut.RowCount)
	assert.True(t, cut.Truncated)
	rows := decodeRows(t, cut)
	assert.Equal(t, 3000, len(rows))
	assert.Equal(t, float64(2999), rows[2999]["n"])

	exact := encodeSQL(t, conn, "SELECT range AS n FROM range(5)", 5)
	assert.Equal(t, 5, exact.RowCount)
	assert.False(t, exact.Truncated)

	empty := encodeSQL(t, conn, "SELECT range AS n FROM range(0)", 5)
	assert.Equal(t, 0, empty.RowCount)
	assert.Equal(t, "[]", string(empty.Rows))
	assert.Equal(t, 1, len(empty.Columns))
}
