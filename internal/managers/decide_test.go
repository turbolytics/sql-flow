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
	assert.That(t, strings.Contains(err.Error(), "data=none idle=off source=delivering matches no row"))

	// A row duplicated: its combinations match twice.
	watermarkTable = append(append([]watermarkRule{}, saved...), saved[0])
	err = checkTables()
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "matches hold.empty and hold.empty"))

	// A row nothing reaches: a rule for a combination another row already
	// owns, with a value no state carries.
	watermarkTable = append(append([]watermarkRule{}, saved...), watermarkRule{
		Name: "unreachable", Data: []Data{"nowhere"}, Action: Hold,
	})
	err = checkTables()
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "row unreachable is selected by no combination"))
}

// One example per row, each run from raw readings through StateOf, so the
// reduction and the lookup are proven together. The declaration is the
// test's: one-minute buckets, a minute of grace, five minutes idle.
func TestManagerWindow_EveryRuleHasAnExample(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := testDecl()
	wm := t0.Add(10 * time.Minute) // the committed watermark

	examples := []struct {
		name        string
		newest      time.Time
		hasRows     bool
		previous    time.Time
		hadPrevious bool
		quiet       time.Duration
		rule        string
		action      Action
	}{
		{"no rows, however quiet", time.Time{}, false, wm, true, time.Hour, "hold.empty", Hold},
		{"newest bucket ended at the watermark", wm.Add(-decl.Size), true, wm, true, time.Hour, "hold.behind", Hold},
		{"newest bucket open, grace not past, stream not confirmed quiet", wm, true, wm, true, 4 * time.Minute, "hold.open", Hold},
		{"newest bucket open and the engine confirmed five minutes quiet", wm, true, wm, true, 5 * time.Minute, "close.idle", CloseByIdle},
		{"grace past and the engine confirmed five minutes quiet", wm.Add(time.Hour), true, wm, true, 5 * time.Minute, "close.idle", CloseByIdle},
		{"grace past, stream not confirmed quiet", wm.Add(decl.Grace + time.Second), true, wm, true, 0, "close.grace", CloseByGrace},
		{"first rows, never closed", t0, true, time.Time{}, false, 0, "close.grace", CloseByGrace},
	}
	for _, ex := range examples {
		t.Run(ex.name, func(t *testing.T) {
			state := StateOf(decl, ex.newest, ex.hasRows, ex.previous, ex.hadPrevious, ex.quiet, time.Hour, true)
			assert.Equal(t, ex.rule, watermarkRuleFor(state).Name)
			assert.Equal(t, ex.action, Decide(state))
		})
	}

	// A window with no idle bound never confirms anything.
	off := decl
	off.IdleClose = 0
	assert.Equal(t, IdleOff, StateOf(off, wm, true, wm, true, time.Hour, time.Hour, true).Idle)
	assert.Equal(t, Hold, Decide(StateOf(off, wm, true, wm, true, time.Hour, time.Hour, true)))
}

// The boundaries compare the way the SQL always has: a bucket that ends
// exactly at the watermark is closed, a grace that lands exactly on it has
// not passed, and quiet equal to the bound is confirmed.
func TestManagerWindow_TheBoundariesAreExact(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := testDecl()
	wm := t0.Add(10 * time.Minute)

	assert.Equal(t, DataBehind, StateOf(decl, wm.Add(-decl.Size), true, wm, true, 0, time.Hour, true).Data)
	assert.Equal(t, DataOpen, StateOf(decl, wm.Add(-decl.Size+time.Microsecond), true, wm, true, 0, time.Hour, true).Data)
	assert.Equal(t, DataOpen, StateOf(decl, wm.Add(decl.Grace), true, wm, true, 0, time.Hour, true).Data)
	assert.Equal(t, DataRipe, StateOf(decl, wm.Add(decl.Grace+time.Microsecond), true, wm, true, 0, time.Hour, true).Data)
	assert.Equal(t, IdleUnconfirmed, StateOf(decl, wm, true, wm, true, decl.IdleClose-time.Microsecond, time.Hour, true).Idle)
	assert.Equal(t, IdleConfirmed, StateOf(decl, wm, true, wm, true, decl.IdleClose, time.Hour, true).Idle)

	end := wm.Add(time.Hour)
	assert.Equal(t, BucketLate, BucketStateOf(decl, wm, wm, end).Bucket)
	assert.Equal(t, BucketDue, BucketStateOf(decl, wm.Add(time.Microsecond), wm, end).Bucket)
	assert.Equal(t, BucketDue, BucketStateOf(decl, end, wm, end).Bucket)
	assert.Equal(t, BucketOpen, BucketStateOf(decl, end.Add(time.Microsecond), wm, end).Bucket)
}

// window.never_backwards: no reading moves the watermark behind the
// committed one. window.idle_beats_grace: confirmed quiet with anything
// open closes by idle, which is never lower than the grace close. Both are
// checked over random readings, the second only where both closes apply.
func TestManagerWindow_InvariantsHoldOverRandomReadings(t *testing.T) {
	coverage.Covers(t, "manager.window")
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10000; i++ {
		decl := Declaration{
			Size:      time.Duration(1+rng.Intn(3600)) * time.Second,
			Grace:     time.Duration(rng.Intn(7200)) * time.Second,
			IdleClose: time.Duration(rng.Intn(600)) * time.Second,
		}
		previous := t0.Add(time.Duration(rng.Intn(86400)) * time.Second)
		newest := t0.Add(time.Duration(rng.Intn(86400)) * time.Second)
		hadPrevious := rng.Intn(4) != 0
		hasRows := rng.Intn(8) != 0
		quiet := time.Duration(rng.Intn(1200)) * time.Second
		// The third fact is drawn too, so the watermark's monotonicity is
		// checked across a source that comes and goes.
		deliveringFor := time.Duration(rng.Intn(1200)) * time.Second
		delivering := rng.Intn(8) != 0

		state := StateOf(decl, newest, hasRows, previous, hadPrevious, quiet, deliveringFor, delivering)
		action := Decide(state)
		next, moved := action.Next(decl, newest, previous)

		if hadPrevious && next.Before(previous) {
			t.Fatalf("window.never_backwards: %v moved %s to %s from %s", state, action, next, previous)
		}
		if moved == (action == Hold) {
			t.Fatalf("moved=%v for %s", moved, action)
		}
		if state.Idle == IdleConfirmed && (state.Data == DataOpen || state.Data == DataRipe) {
			if action != CloseByIdle {
				t.Fatalf("window.idle_beats_grace: %v decided %s", state, action)
			}
			if byGrace, _ := CloseByGrace.Next(decl, newest, previous); next.Before(byGrace) {
				t.Fatalf("window.idle_beats_grace: idle close %s is below the grace close %s", next, byGrace)
			}
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

## The watermark, once per poll

| Fact | Values | Computed from |
|---|---|---|
| ` + "`data`" + ` | ` + "`none`, `behind`, `open`, `ripe`" + ` | The newest bucket's start, in event time, against the committed watermark. ` + "`none`" + `: no rows. ` + "`behind`" + `: newest + size is at or before the watermark, so everything held has closed. ` + "`open`" + `: newest + size is after the watermark and newest - grace is not. ` + "`ripe`" + `: newest - grace is after the watermark, or there is no watermark yet. |
| ` + "`idle`" + ` | ` + "`off`, `unconfirmed`, `confirmed`" + ` | ` + "`idle_close_seconds`" + ` and the engine's progress row. ` + "`off`" + ` when not declared. ` + "`confirmed`" + ` when ` + "`last_commit - last_arrival`" + ` is at least the bound. ` + "`unconfirmed`" + ` otherwise, including a row the engine has not written. |

`)
	fmt.Fprintf(&b, "%d combinations, %d rows. A blank cell matches every value.\n\n", len(allStates()), len(watermarkTable))
	b.WriteString("| Rule | data | idle | Action | Watermark | Deciding | Claim |\n|---|---|---|---|---|---|---|\n")
	for _, r := range watermarkTable {
		where := map[Action]string{Hold: "unchanged", CloseByGrace: "newest - grace", CloseByIdle: "newest + size"}[r.Action]
		fmt.Fprintf(&b, "| `%s` | %s | %s | `%s` | %s | %s | %s |\n",
			r.Name, joinValues(r.Data), joinValues(r.Idle), r.Action, where, r.Deciding, r.Claim)
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
## Invariants

Each is a claim with a check in the tests.

| ID | Claim |
|---|---|
| ` + "`window.never_backwards`" + ` | No reading moves the watermark behind the committed one. Checked over ten thousand random readings. |
| ` + "`window.idle_beats_grace`" + ` | Confirmed quiet with anything open closes by idle, and that close is never below the grace close. Checked over the same readings. |
| ` + "`progress.quiet_is_watched`" + ` | The progress row never confirms more quiet than the engine spent waiting on a source that could deliver: a restart, a clock step, a sink write held in retries, a consumer between groups and a websocket reconnecting are not quiet, on the idle tick and on the drain alike. Checked in ` + "`internal/core`" + `, ` + "`internal/kafka`" + ` and ` + "`internal/websocket`" + `. |
| ` + "`progress.late_never_early`" + ` | A progress write that is late, or fails, delays a close and never advances one. Checked in ` + "`internal/core`" + ` and here. |
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
