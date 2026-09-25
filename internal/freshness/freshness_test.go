package freshness

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A bucket's data is as old as the bucket's end: an hour bucket that
// started at 18:00 is complete at 19:00.
func TestCliRollupRun_AgeRunsFromTheNewestBucketsEnd(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	newest := time.Date(2026, 9, 24, 18, 0, 0, 0, time.UTC)
	o := Observation{Grain: time.Hour, NewestBucketAt: &newest, ObservedAt: newest.Add(time.Hour + 41*time.Minute)}
	age, ok := o.Age()
	assert.True(t, ok)
	assert.Equal(t, 41*time.Minute, age)

	_, ok = Observation{Grain: time.Hour}.Age()
	assert.False(t, ok)
}

// The two forms of a store's id, fixed so a reporter built from another
// version names the same store the same way.
func TestCliRollupRun_AStoreIDHasTwoForms(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	assert.Equal(t, "pg:71b9f0d711eccc11", systemID(7412345678901234567, "rollup"))
	assert.Equal(t, "pg:f431cc3d5ad5f18e", addressID("172.17.0.2", 5432, "rollup"))
}
