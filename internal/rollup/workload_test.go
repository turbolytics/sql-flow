package rollup

import (
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// Each type the workload writes draws from its own range, and a timestamp
// lands inside the row's source bucket.
func TestCliRollupTest_EachTypeGeneratesItsValues(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	rng := rand.New(rand.NewSource(1))
	start := time.Date(2026, 3, 8, 6, 59, 0, 0, time.UTC)
	for i := 0; i < 500; i++ {
		for _, typ := range []string{"smallint", "integer", "bigint"} {
			v, err := generate(sourceColumn{Name: "n", Type: typ}, start, time.Minute, rng)
			assert.NoError(t, err)
			n, err := strconv.ParseInt(v.(string), 10, 64)
			assert.NoError(t, err)
			assert.That(t, n >= 0 && n <= 1000)
		}

		// Never negative: a sum of mixed signs can land near zero, where
		// verify's relative tolerance fails a correct merge.
		v, err := generate(sourceColumn{Name: "x", Type: "double precision"}, start, time.Minute, rng)
		assert.NoError(t, err)
		f, err := strconv.ParseFloat(v.(string), 64)
		assert.NoError(t, err)
		assert.That(t, f >= 0 && f <= 1000 && f != math.Trunc(f))

		for _, typ := range []string{"text", "character varying(8)"} {
			v, err = generate(sourceColumn{Name: "s", Type: typ}, start, time.Minute, rng)
			assert.NoError(t, err)
			assert.That(t, slices.Contains(textPool, v.(string)))
		}

		for _, typ := range []string{"timestamp with time zone", "timestamp without time zone"} {
			v, err = generate(sourceColumn{Name: "at", Type: typ}, start, time.Minute, rng)
			assert.NoError(t, err)
			at := v.(time.Time)
			assert.That(t, !at.Before(start) && at.Before(start.Add(time.Minute)))
		}
	}
}

// A type the workload cannot write stops the run, naming the column and its
// type.
func TestCliRollupTest_AnUnknownTypeStopsTheRun(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	_, err := generate(sourceColumn{Name: "payload", Type: "bytea"}, time.Unix(0, 0), time.Minute, rand.New(rand.NewSource(1)))
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "payload") && strings.Contains(err.Error(), "bytea"))
}

// The statement is the sink's upsert: every written column binds as text
// cast to its type, and the columns outside the key update from EXCLUDED.
func TestCliRollupTest_TheUpsertIsTheSinks(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	cols := []sourceColumn{
		{Name: "bucket", Type: "timestamp with time zone"},
		{Name: "lang", Type: "text"},
		{Name: "posts", Type: "integer"},
	}
	assert.Equal(t,
		`INSERT INTO "posts_per_minute_by_lang" ("bucket", "lang", "posts") VALUES `+
			`($1::timestamp with time zone, $2::text, $3::integer), ($4::timestamp with time zone, $5::text, $6::integer) `+
			`ON CONFLICT ("bucket", "lang") DO UPDATE SET "posts" = EXCLUDED."posts"`,
		upsertSQL("posts_per_minute_by_lang", cols, []string{"bucket", "lang"}, 2))
	assert.Equal(t,
		`INSERT INTO "t" ("bucket") VALUES ($1::timestamp with time zone) ON CONFLICT ("bucket") DO NOTHING`,
		upsertSQL("t", cols[:1], []string{"bucket"}, 1))
}
