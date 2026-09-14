package config

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	"gopkg.in/yaml.v3"
)

// The postgres block decodes with the four fields the spec names, and the
// sink is one the retry ladder wraps: it crosses a network to somebody
// else's server.
func TestSinkPostgres_ConfigBlockDecodes(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	var s Sink
	assert.NoError(t, yaml.Unmarshal([]byte(`
type: postgres
postgres:
  dsn: postgres://u:p@localhost:5432/db
  table: public.rollups
  mode: upsert
  key: [bucket, lang]
`), &s))
	assert.Equal(t, "postgres", s.Type)
	assert.Equal(t, "postgres://u:p@localhost:5432/db", s.Postgres.DSN)
	assert.Equal(t, "public.rollups", s.Postgres.Table)
	assert.Equal(t, "upsert", s.Postgres.Mode)
	assert.DeepEqual(t, []string{"bucket", "lang"}, s.Postgres.Key)
	assert.That(t, SinkRetries("postgres"))
}
