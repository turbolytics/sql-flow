package managers

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The package refuses to load with a broken table, so this asserts the
// checker itself: each fault it must catch, caught.
func TestManagerWindow_TheTableCheckerCatchesEveryFault(t *testing.T) {
	coverage.Covers(t, "manager.window")
	saved := watermarkTable
	defer func() { watermarkTable = saved }()

	assert.NoError(t, checkTables())

	// A row removed: the combinations it covered match nothing.
	watermarkTable = saved[1:]
	err := checkTables()
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "asserted=none matches no row"))

	// A row duplicated: its combinations match twice.
	watermarkTable = append(append([]watermarkRule{}, saved...), saved[0])
	err = checkTables()
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "matches hold.unasserted and hold.unasserted"))

	// A row nothing reaches: a rule for a value no state carries.
	watermarkTable = append(append([]watermarkRule{}, saved...), watermarkRule{
		Name: "unreachable", Asserted: []Asserted{"nowhere"}, Action: Hold,
	})
	err = checkTables()
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "row unreachable is selected by no combination"))
}

// One example per row, each run from raw readings through StateOf, so the
// reduction and the lookup are proven together.
func TestManagerWindow_EveryRuleHasAnExample(t *testing.T) {
	coverage.Covers(t, "manager.window")
	closed := t0.Add(10 * time.Minute)

	examples := []struct {
		name        string
		asserted    time.Time
		hasAsserted bool
		closed      time.Time
		hadClosed   bool
		rule        string
		action      Action
	}{
		{"nothing asserted, nothing closed", time.Time{}, false, time.Time{}, false, "hold.unasserted", Hold},
		{"nothing asserted, something closed", time.Time{}, false, closed, true, "hold.unasserted", Hold},
		{"asserted where the window closed", closed, true, closed, true, "hold.behind", Hold},
		{"asserted below where the window closed", closed.Add(-time.Minute), true, closed, true, "hold.behind", Hold},
		{"asserted past where the window closed", closed.Add(time.Minute), true, closed, true, "follow", Follow},
		{"asserted, never closed", t0, true, time.Time{}, false, "follow", Follow},
	}
	for _, ex := range examples {
		t.Run(ex.name, func(t *testing.T) {
			state := StateOf(ex.asserted, ex.hasAsserted, ex.closed, ex.hadClosed)
			assert.Equal(t, ex.rule, watermarkRuleFor(state).Name)
			assert.Equal(t, ex.action, Decide(state))
		})
	}
}

// The boundaries compare the way the SQL always has: an assertion exactly at
// the closed watermark is behind, a microsecond past it is ahead; a bucket
// that ends exactly at the watermark is closed.
func TestManagerWindow_TheBoundariesAreExact(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := testDecl()
	closed := t0.Add(10 * time.Minute)

	assert.Equal(t, AssertedBehind, StateOf(closed, true, closed, true).Asserted)
	assert.Equal(t, AssertedAhead, StateOf(closed.Add(time.Microsecond), true, closed, true).Asserted)

	end := closed.Add(time.Hour)
	assert.Equal(t, BucketLate, BucketStateOf(decl, closed, closed, end).Bucket)
	assert.Equal(t, BucketDue, BucketStateOf(decl, closed.Add(time.Microsecond), closed, end).Bucket)
	assert.Equal(t, BucketDue, BucketStateOf(decl, end, closed, end).Bucket)
	assert.Equal(t, BucketOpen, BucketStateOf(decl, end.Add(time.Microsecond), closed, end).Bucket)
}

// window.never_backwards: no reading moves the closed watermark behind the
// committed one. And a follow lands exactly on the assertion, never past
// it: the manager adds nothing of its own to what the engine promised.
func TestManagerWindow_InvariantsHoldOverRandomReadings(t *testing.T) {
	coverage.Covers(t, "manager.window")
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10000; i++ {
		closed := t0.Add(time.Duration(rng.Intn(86400)) * time.Second)
		asserted := t0.Add(time.Duration(rng.Intn(86400)) * time.Second)
		hadClosed := rng.Intn(4) != 0
		hasAsserted := rng.Intn(4) != 0

		state := StateOf(asserted, hasAsserted, closed, hadClosed)
		action := Decide(state)
		next, moved := action.Next(asserted, closed)

		if hadClosed && next.Before(closed) {
			t.Fatalf("window.never_backwards: %v moved %s to %s from %s", state, action, next, closed)
		}
		if moved == (action == Hold) {
			t.Fatalf("moved=%v for %s", moved, action)
		}
		if moved && !next.Equal(asserted) {
			t.Fatalf("%v: followed to %s, not the assertion %s", state, next, asserted)
		}
		if hasAsserted && (!hadClosed || asserted.After(closed)) && action != Follow {
			t.Fatalf("%v: an assertion ahead of the window was held", state)
		}
	}
}

const decisionsDoc = "../../docs/windows/decisions.md"

// The published tables are rendered from the ones that run. UPDATE_GOLDEN=1
// rewrites the document; otherwise it has to match.
func TestManagerWindow_ThePublishedDecisionsAreTheTables(t *testing.T) {
	coverage.Covers(t, "manager.window")
	rendered := renderDecisions()

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		assert.NoError(t, os.WriteFile(decisionsDoc, []byte(rendered), 0o644))
		t.Log("decisions doc updated")
		return
	}
	raw, err := os.ReadFile(decisionsDoc)
	assert.NoError(t, err)
	if string(raw) != rendered {
		t.Fatalf("%s is not what the tables render; run UPDATE_GOLDEN=1 go test ./internal/managers -run ThePublishedDecisions", decisionsDoc)
	}
}

func renderDecisions() string {
	var b strings.Builder
	b.WriteString(`# Window decisions

Rendered from the truth tables in ` + "`internal/managers/decide.go`" + ` by a test, so
this page is what runs. A poll reduces what it read to one value per fact,
looks the combination up, and performs the action of the one row it
selects. The check that runs when the package loads proves each combination
selects exactly one row and every row is reachable.

The engine asserts each window's watermark, in the commit that makes the
rows it describes visible: as of that commit, every row the pipeline will
ever write for the window has ` + "`time_column`" + ` at or after the watermark. How it
is computed -- the newest event time seen per source partition, less the
grace, combined by minimum over the partitions that could still deliver --
is ` + "`internal/core/watermarks.go`" + `. The manager reads that one value and
nothing else: no clock, no progress row, no reading of the table's newest
bucket. See ` + "`docs/superpowers/specs/2026-09-24-window-watermark-design.md`" + `.

## The watermark, once per poll

| Fact | Values | Computed from |
|---|---|---|
| ` + "`asserted`" + ` | ` + "`none`, `behind`, `ahead`" + ` | The engine's watermark in ` + "`sqlflow_watermarks`" + ` against the closed watermark in ` + "`sqlflow_windows`" + `, both event time. ` + "`none`" + `: the engine has asserted nothing for this window. ` + "`behind`" + `: the assertion is at or before the closed watermark, so it has been acted on. ` + "`ahead`" + `: the assertion is past the closed watermark, or nothing has closed yet. |

`)
	fmt.Fprintf(&b, "%d combinations, %d rows.\n\n", len(allStates()), len(watermarkTable))
	b.WriteString("| Rule | asserted | Action | Watermark | Deciding | Claim |\n|---|---|---|---|---|---|\n")
	for _, r := range watermarkTable {
		where := map[Action]string{Hold: "unchanged", Follow: "the assertion"}[r.Action]
		fmt.Fprintf(&b, "| `%s` | %s | `%s` | %s | %s | %s |\n",
			r.Name, joinValues(r.Asserted), r.Action, where, r.Deciding, r.Claim)
	}

	b.WriteString(`
## Each bucket, given the poll's watermark

| Fact | Values | Computed from |
|---|---|---|
| ` + "`bucket`" + ` | ` + "`late`, `due`, `open`" + ` | The bucket's end against the previous watermark and the one this poll decided. ` + "`late`" + `: at or before the previous, so its rows arrived after it closed. ` + "`due`" + `: after the previous and at or before the next. ` + "`open`" + `: after the next. |
| ` + "`policy`" + ` | ` + "`drop`, `reemit`" + ` | ` + "`late_rows`" + ` in the window's config. |

`)
	fmt.Fprintf(&b, "%d combinations, %d rows.\n\n", len(allBucketStates()), len(bucketTable))
	b.WriteString("| Rule | bucket | policy | Action | Deciding | Claim |\n|---|---|---|---|---|---|\n")
	for _, r := range bucketTable {
		fmt.Fprintf(&b, "| `%s` | %s | %s | `%s` | %s | %s |\n",
			r.Name, joinValues(r.Bucket), joinValues(r.Policy), r.Action, r.Deciding, r.Claim)
	}

	b.WriteString(`
## What closes a bucket, by configuration

Three keys decide. ` + "`grace_seconds`" + ` is how far the stream's own clock must
pass a bucket's end before it closes (Flink's bounded out-of-orderness);
` + "`idle_close_seconds`" + ` is how long a partition may be silent before it stops
holding the window open (Flink's idleness); ` + "`late_rows`" + ` is what happens to a
row for a bucket that already closed.

| | ` + "`grace_seconds`" + ` | ` + "`idle_close_seconds`" + ` | the stream this is for | what closes a bucket | the failure mode to know |
|---|---|---|---|---|---|
| **A** | 0 | absent | an ordered stream that never stops | only event time moving past the bucket's end | a stream that stops never publishes its last bucket |
| **B** | >0 | absent | a reordered stream that never stops | event time, less the grace | the same, and the tail is ` + "`grace`" + ` further behind |
| **C** | 0 | set | ordered, intermittent | event time, or every partition silent for the bound | a bound shorter than the gap *within* a burst closes mid-burst, and the rest of the burst is late |
| **D** | >0 | set | bursty and reordered: the IoT default | either | both of the above |

Each crossed with ` + "`drop`" + ` (a late row is discarded and counted) or ` + "`reemit`" + `
(a late row is republished on its own, which a replacing sink turns into
loss).

## Invariants

Each is a claim with a check in the tests.

| ID | Claim |
|---|---|
| ` + "`window.never_backwards`" + ` | No reading moves the closed watermark behind the committed one, checked over ten thousand random readings here; and the engine's assertion is ` + "`max(stored, W)`" + ` by construction, checked in ` + "`internal/core`" + `. |
| ` + "`window.asserted_in_the_commit`" + ` | The engine writes the watermark in the transaction that commits the rows it describes, so no reader can see rows without the watermark that accounts for them, or a watermark without its rows. A commit that fails leaves both where they were. Checked in ` + "`internal/core`" + `. |
| ` + "`window.minimum_over_partitions`" + ` | The watermark is the minimum over the partitions that could still deliver: one that has not delivered holds it at -inf, a lagging one holds it, an idle one leaves it, a lost one holds at its last position, a revoked one is gone. Checked in ` + "`internal/core`" + ` and by the simulator. |
| ` + "`window.no_clock_in_the_manager`" + ` | The manager has no clock. Every close is ` + "`bucket end <= asserted watermark`" + `, in event time; the engine's clock decides only which partitions are in its minimum. Checked by construction: the manager takes no clock. |
`)
	return b.String()
}

func joinValues[T ~string](vals []T) string {
	if len(vals) == 0 {
		return ""
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = "`" + string(v) + "`"
	}
	return strings.Join(parts, ", ")
}

// The bucket table's boundary is the SQL's. Production splits late from
// due with closedBefore, and BucketStateOf is the same comparison in Go;
// this runs both at the boundary and a microsecond past it so a change to
// either fails here.
func TestManagerWindow_TheBucketBoundaryIsTheSQLs(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	decl := testDecl()

	// One bucket, starting at t0 and ending at t0 + size.
	insertBucket(t, d.pipeline, 0, "NYC", 1)
	end := bucket(0).Add(decl.Size)
	next := end.Add(time.Hour)

	for _, c := range []struct {
		name      string
		watermark time.Time
		want      Bucket
	}{
		{"watermark a microsecond before the end", end.Add(-time.Microsecond), BucketDue},
		{"watermark at the end", end, BucketLate},
		{"watermark a microsecond past the end", end.Add(time.Microsecond), BucketLate},
	} {
		t.Run(c.name, func(t *testing.T) {
			closed, _, err := queryInt64(ctx, d.pipeline, decl.countClosedSQL(c.watermark))
			assert.NoError(t, err)
			got := BucketStateOf(decl, end, c.watermark, next).Bucket
			assert.Equal(t, c.want, got)
			assert.Equal(t, got == BucketLate, closed == 1)
		})
	}
}
