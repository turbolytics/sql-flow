package core

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/zeebo/assert"
)

// refConn opens an in-memory DuckDB. The AST walk parses through DuckDB
// itself, in the dialect and version that will run the pipeline, so there is
// nothing to fake here.
func refConn(t *testing.T) adbc.Connection {
	t.Helper()
	return newStateConn(t, "")
}

// TestReferenceTablesFindsNestedJoin uses the csv.mem.join.yml handler SQL,
// whose join lives inside a subquery. A walk that only looked at the top-level
// FROM would miss the one table that matters.
func TestReferenceTablesFindsNestedJoin(t *testing.T) {
	conn := refConn(t)
	sql := `SELECT properties.city, state_full FROM batch
	        LEFT JOIN (SELECT * FROM locations
	                   WHERE locations.city = properties.city LIMIT 1) AS l
	        ON l.city = properties.city`

	got, err := ReferenceTables(context.Background(), conn, sql, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
	assert.Equal(t, "locations", got[0].Qualified())
}

// TestReferenceTablesExcludesCTEs is the false-positive case. DuckDB emits a
// CTE reference as a BASE_TABLE node, so a naive walk would try to count a
// name that does not exist outside its own query and warn on every start of
// every pipeline that uses WITH.
func TestReferenceTablesExcludesCTEs(t *testing.T) {
	conn := refConn(t)
	sql := `WITH recent AS (SELECT * FROM batch WHERE ts > now())
	        SELECT r.city, l.state FROM recent r
	        JOIN locations l ON l.city = r.city`

	got, err := ReferenceTables(context.Background(), conn, sql, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
	assert.Equal(t, "locations", got[0].Qualified())
}

// TestReferenceTablesExcludesManagedTables keeps a tumbling window's aggregate
// table out of the set. Those legitimately start empty; a dimension does not.
func TestReferenceTablesExcludesManagedTables(t *testing.T) {
	conn := refConn(t)
	sql := `SELECT * FROM batch JOIN agg_city_count a ON a.city = batch.city`

	got, err := ReferenceTables(context.Background(), conn, sql,
		[]string{"agg_city_count"})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(got))
}

// TestReferenceTablesKeepsCatalogQualification separates an attached table
// from a local one, which decides whether it is counted or probed.
func TestReferenceTablesKeepsCatalogQualification(t *testing.T) {
	conn := refConn(t)
	sql := `SELECT * FROM batch LEFT JOIN pgusersdb.public.users u ON u.id = batch.user_id`

	got, err := ReferenceTables(context.Background(), conn, sql, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
	assert.Equal(t, "pgusersdb", got[0].Catalog)
	assert.That(t, got[0].Attached())
	assert.Equal(t, "pgusersdb.public.users", got[0].Qualified())
}

// TestReferenceTablesDeduplicates keeps one table joined twice from being
// counted twice at startup.
func TestReferenceTablesDeduplicates(t *testing.T) {
	conn := refConn(t)
	sql := `SELECT * FROM batch
	        JOIN locations a ON a.city = batch.city
	        JOIN locations b ON b.city = batch.other_city`

	got, err := ReferenceTables(context.Background(), conn, sql, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
}

// TestReferenceTablesUnparseableSQL returns an error rather than an empty set.
// An empty set and a query that could not be read are different facts, and
// silently reporting no reference tables is how a check stops checking.
func TestReferenceTablesUnparseableSQL(t *testing.T) {
	conn := refConn(t)
	_, err := ReferenceTables(context.Background(), conn, "SELECT FROM WHERE", nil)
	assert.Error(t, err)
}
