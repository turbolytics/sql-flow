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
