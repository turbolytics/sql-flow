package daemon

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A leader that has not read its tables to fill does not know what is
// pending, and a rollup with no backfill reads as filled on the wire. So it
// lists no rollups until the read, as /healthz waits for the same read. After
// the read, a pending table appears as backfill.
func TestCliRollupRun_ANewLeaderReportsNoRollupsBeforeItReadsItsTables(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	conf := loadExample(t)
	r := newReport(conf)

	before, _ := r.section(roleLeader, false, conf.Rollups)
	assert.Equal(t, roleLeader, before.Role)
	assert.Equal(t, 0, len(before.Rollups))

	r.setPending(map[string][]string{"posts": {"posts_by_lang_5m"}})
	after, _ := r.section(roleLeader, true, conf.Rollups)
	assert.Equal(t, 1, len(after.Rollups))
	assert.That(t, after.Rollups[0].Backfill != nil)
	assert.Equal(t, 1, after.Rollups[0].Backfill.TablesLeft)
}
