package serve

import (
	"context"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// SET TimeZone is session-scoped, so a session the executor did not pin
// inherits the host's zone and evaluates date_trunc and a naive cast in it —
// wrong buckets from a correct config, on some requests and not others.
//
// The test runs under a non-UTC TZ on purpose: under UTC a missing pin looks
// correct, which is how this would reach production unnoticed.
func TestCliServe_EverySessionIsUTC(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	t.Setenv("TZ", "America/New_York")

	ctx := context.Background()
	const size = 4
	ex, _ := newExec(t, size)

	st, err := ex.Prepare(ctx, StatementSpec{
		Dataset: "probe",
		SQL:     "SELECT current_setting('TimeZone') AS zone",
	})
	assert.NoError(t, err)

	// Hold every session at once, so each one is checked rather than the
	// same one four times.
	held := make([]Session, 0, size)
	for i := 0; i < size; i++ {
		s, err := ex.Acquire(ctx)
		assert.NoError(t, err)
		held = append(held, s)
	}
	defer func() {
		for _, s := range held {
			s.Release()
		}
	}()

	for i, s := range held {
		rdr, err := s.Run(ctx, st, nil)
		assert.NoError(t, err)
		res, err := readRows(rdr, 1)
		rdr.Release()
		assert.NoError(t, err)

		// res.Rows is the JSON array of row objects, so the zone is a value
		// inside it rather than the whole string.
		if !strings.Contains(string(res.Rows), `"UTC"`) {
			t.Fatalf("session %d is not pinned to UTC: %s", i, res.Rows)
		}
	}
}
