package wire

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The spec's example, byte for byte after a round trip: every field the
// section defines is here, and nothing a kind cannot provide is forced to
// zero.
func TestDatabase_SpecExampleRoundTrips(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	at := time.Date(2026, 10, 5, 11, 59, 59, 0, time.UTC)
	d := Database{
		Kind: "postgres", Target: "pg-2.internal:5432/billing", Cluster: "billing", ServerVersion: "18.0",
		Probe: DatabaseProbe{OK: true, LatencyMs: 3, LastOKAt: &at},
		Resources: &DatabaseResources{
			Connections:              &DatabaseConnections{Used: 18, Max: 100, Waiting: 0},
			SizeBytes:                dbPtr(int64(14104255)),
			OldestTransactionSeconds: dbPtr(int64(2)),
			Memory:                   &DatabaseMemory{SharedBuffersBytes: 134217728},
		},
		Tables: []DatabaseTable{{
			Name: "public.usage_per_minute", FreshnessColumn: "minute", NewestAt: &at,
			Rows: dbPtr(int64(7832)), RowsExact: false, SizeBytes: 1826816, LastVacuumAt: &at, CheckedAt: at,
		}},
		Replication: &DatabaseReplication{Role: "replica", LagSeconds: dbPtr(0.4), LastReplayedAt: &at, Upstream: "pg-1.internal:5432"},
		Collection:  DatabaseCollection{Queries: 9, DurationMs: 41, Errors: []DatabaseError{}},
	}
	b, err := json.Marshal(d)
	assert.NoError(t, err)
	var back Database
	assert.NoError(t, json.Unmarshal(b, &back))
	assert.Equal(t, d, back)
	// Field names are the spec's.
	for _, want := range []string{`"kind":"postgres"`, `"latency_ms":3`, `"rows_exact":false`, `"lag_seconds":0.4`, `"duration_ms":41`, `"size_bytes":14104255`} {
		assert.That(t, strings.Contains(string(b), want))
	}
}

// A failed probe sends probe alone: nothing else could run.
func TestDatabase_FailedProbeCarriesProbeAlone(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	d := Database{Kind: "postgres", Target: "pg:5432/x", Probe: DatabaseProbe{OK: false, ConsecutiveFailures: 3, Error: "refused"}}
	b, err := json.Marshal(d)
	assert.NoError(t, err)
	for _, absent := range []string{`"resources"`, `"tables"`, `"replication"`} {
		assert.That(t, !strings.Contains(string(b), absent))
	}
	assert.That(t, strings.Contains(string(b), `"error":"refused"`))
	assert.That(t, strings.Contains(string(b), `"consecutive_failures":3`))
}

// A bundle with a database section and no pipeline is the shape dbhealth
// sends; the field sits beside Pipeline and Serve, omitted when nil.
func TestBundle_DatabaseSectionIsOptional(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	b, err := json.Marshal(Bundle{V: 1, Database: &Database{Kind: "postgres", Target: "pg:5432/x", Probe: DatabaseProbe{OK: true}}})
	assert.NoError(t, err)
	assert.That(t, strings.Contains(string(b), `"database":{`))
	assert.That(t, !strings.Contains(string(b), `"pipeline"`))
	b, err = json.Marshal(Bundle{V: 1})
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(b), `"database"`))
}

func dbPtr[T any](v T) *T { return &v }

// The load amendment's example, field for field. A kind that cannot say
// a field omits it; the names are the spec's.
func TestDatabaseLoad_SpecExampleRoundTrips(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	raw := `{
	  "sessions_active_now": 12, "sessions_idle_in_transaction_now": 3, "sessions_waiting_now": 1,
	  "queries_queued_now": 0, "longest_query_seconds": 41.2,
	  "queries_per_second": 1240.5, "transactions_per_second": 182.4, "rollbacks_per_second": 0.3,
	  "rows_read_per_second": 90210, "rows_written_per_second": 1850, "bytes_scanned_per_second": 0,
	  "cache_hit_ratio": 0.993, "deadlocks_per_second": 0, "temp_bytes_per_second": 0
	}`
	var l DatabaseLoad
	assert.NoError(t, json.Unmarshal([]byte(raw), &l))
	assert.Equal(t, 12, *l.SessionsActiveNow)
	assert.Equal(t, 41.2, *l.LongestQuerySeconds)
	assert.Equal(t, 0.993, *l.CacheHitRatio)
	assert.Equal(t, 0.0, *l.TempBytesPerSecond)
	out, err := json.Marshal(l)
	assert.NoError(t, err)
	var back map[string]any
	assert.NoError(t, json.Unmarshal(out, &back))
	for _, k := range []string{"sessions_active_now", "sessions_idle_in_transaction_now", "sessions_waiting_now",
		"queries_queued_now", "longest_query_seconds", "queries_per_second", "transactions_per_second",
		"rollbacks_per_second", "rows_read_per_second", "rows_written_per_second", "bytes_scanned_per_second",
		"cache_hit_ratio", "deadlocks_per_second", "temp_bytes_per_second"} {
		if _, ok := back[k]; !ok {
			t.Errorf("marshalled load lacks %q", k)
		}
	}
	assert.Equal(t, 14, len(back))

	q := `{"id":"a7f3","text":"SELECT * FROM events WHERE customer = $1","calls_per_second":310.2,"mean_ms":2.4,"time_share":0.31,"rows_per_call":18}`
	var dq DatabaseQuery
	assert.NoError(t, json.Unmarshal([]byte(q), &dq))
	assert.Equal(t, "a7f3", dq.ID)
	assert.Equal(t, 0.31, dq.TimeShare)
	out, err = json.Marshal(dq)
	assert.NoError(t, err)
	assert.Equal(t, q, string(out))

	tbl := `{"name":"public.events","rows_exact":false,"size_bytes":1,"checked_at":"2026-10-06T12:00:00Z","dead_rows":1203,"seq_scans_per_second":0.1,"index_scans_per_second":44}`
	var dt DatabaseTable
	assert.NoError(t, json.Unmarshal([]byte(tbl), &dt))
	assert.Equal(t, int64(1203), *dt.DeadRows)
	assert.Equal(t, 44.0, *dt.IndexScansPerSecond)
}

// A rate the interval could not compute is absent, never zero: the first
// interval and a reset send the samples alone.
func TestDatabaseLoad_AbsentRatesAreAbsent(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	n := 12
	out, err := json.Marshal(DatabaseLoad{SessionsActiveNow: &n})
	assert.NoError(t, err)
	assert.Equal(t, `{"sessions_active_now":12}`, string(out))
	d := Database{Kind: "postgres", Target: "h:5432/d", Collection: DatabaseCollection{Errors: []DatabaseError{}}}
	out, err = json.Marshal(d)
	assert.NoError(t, err)
	assert.False(t, strings.Contains(string(out), `"load"`))
	assert.False(t, strings.Contains(string(out), `"queries":[`))
	assert.Equal(t, 20, MaxDatabaseQueries)
}

// A table carries its schema as a hash every report and the changes the
// reporter saw in the report where the hash moved; its writes are rates
// from the system's own counters, like its scans.
func TestDatabaseTable_SchemaAndWritesRoundTrip(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	raw := `{"name":"public.events","rows_exact":false,"size_bytes":1,"checked_at":"2026-10-07T12:00:00Z",` +
		`"schema_hash":"9f2c1a7e4b3d8c05",` +
		`"schema_changes":[{"column":"amount","change":"retyped","from":"integer","to":"numeric(12,2)"},{"column":"legacy_id","change":"dropped","from":"text"},{"column":"region","change":"added","to":"text"}],` +
		`"rows_inserted_per_second":33.4,"rows_updated_per_second":0.2,"rows_deleted_per_second":0}`
	var dt DatabaseTable
	assert.NoError(t, json.Unmarshal([]byte(raw), &dt))
	assert.Equal(t, "9f2c1a7e4b3d8c05", dt.SchemaHash)
	assert.Equal(t, 3, len(dt.SchemaChanges))
	assert.Equal(t, DatabaseSchemaChange{Column: "amount", Change: "retyped", From: "integer", To: "numeric(12,2)"}, dt.SchemaChanges[0])
	assert.Equal(t, 33.4, *dt.RowsInsertedPerSecond)
	assert.Equal(t, 0.0, *dt.RowsDeletedPerSecond)
	out, err := json.Marshal(dt)
	assert.NoError(t, err)
	assert.Equal(t, raw, string(out))

	// A table whose schema did not move sends the hash alone.
	var quiet DatabaseTable
	assert.NoError(t, json.Unmarshal([]byte(`{"name":"public.quiet","rows_exact":false,"size_bytes":1,"checked_at":"2026-10-07T12:00:00Z","schema_hash":"9f2c1a7e4b3d8c05"}`), &quiet))
	out, err = json.Marshal(quiet)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(out), "schema_changes"))
	assert.That(t, !strings.Contains(string(out), "rows_inserted"))
	assert.Equal(t, 60, MaxDatabaseSchemaChanges)
}

// A database counts its tables and how many of them are partitions. A
// table partitioned by day adds a partition a day until retention drops
// one, so a partition count that keeps climbing is retention that stopped.
func TestDatabaseResources_TableAndPartitionCountsRoundTrip(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	raw := `{"size_bytes":247463936,"table_count":172,"partition_count":148}`
	var r DatabaseResources
	assert.NoError(t, json.Unmarshal([]byte(raw), &r))
	assert.Equal(t, 172, *r.TableCount)
	assert.Equal(t, 148, *r.PartitionCount)
	out, err := json.Marshal(r)
	assert.NoError(t, err)
	assert.Equal(t, raw, string(out))

	// A kind that cannot count its tables leaves both out.
	out, err = json.Marshal(DatabaseResources{})
	assert.NoError(t, err)
	assert.Equal(t, `{}`, string(out))
}
