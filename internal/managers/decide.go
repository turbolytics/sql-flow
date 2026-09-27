package managers

import (
	"fmt"
	"strings"
	"time"
)

// Every operation a window performs is a row in one of two tables here.
//
// A pass builds a State from what it read, hands it to Decide, and performs
// the Action it gets back. The tables are the only place a fact meets an
// action, so the logic is read in one place, every combination is accounted
// for, and checkTables proves it at load: each combination selects exactly
// one row, and every row is reachable. The Markdown in
// docs/windows/decisions.md is rendered from these tables by a test, so what
// is published is what runs.
//
// The watermark table has one fact. The engine asserts each window's
// watermark (core.Watermarks), in the commit that makes the rows it
// describes visible, and the manager closes on that and on nothing else: no
// clock, no progress row, no reading of the table's newest bucket. Where
// the assertion stands against what this window has already closed is the
// whole of what a pass decides on.
//
// The bucket table has one fact too: where a bucket's end stands against
// the closed watermark, the asserted one, and the window's lateness. The
// engine decides a record's lateness at arrival against the same
// comparison (core.Watermarks.Classify), so the table here is the manager's
// half of one rule.

// Asserted is where the engine's watermark stands against the closed one,
// in event time.
type Asserted string

const (
	// AssertedNone is a window the engine has asserted nothing for yet.
	AssertedNone Asserted = "none"
	// AssertedBehind is an assertion at or before the closed watermark:
	// everything it covers has already closed.
	AssertedBehind Asserted = "behind"
	// AssertedAhead is an assertion past the closed watermark, or one made
	// before anything has closed.
	AssertedAhead Asserted = "ahead"
)

// State is what one poll knows before it decides.
type State struct {
	Asserted Asserted
}

func (s State) String() string { return fmt.Sprintf("asserted=%s", s.Asserted) }

// Action is what a poll does with the closed watermark.
type Action string

const (
	// Hold leaves it where it is.
	Hold Action = "hold"
	// Follow moves it to the asserted watermark.
	Follow Action = "follow"
)

// Bucket is where one bucket stands against the closed watermark, the
// asserted one, and the window's lateness.
type Bucket string

const (
	// BucketOpen ends after the asserted watermark: its rows stay.
	BucketOpen Bucket = "open"
	// BucketDue ends after the closed watermark and at or before the asserted
	// one: it closes in this pass.
	BucketDue Bucket = "due"
	// BucketRetained ended at or before the closed watermark and its lateness
	// has not run out: published, kept for a late row to republish it whole.
	BucketRetained Bucket = "retained"
	// BucketExpired ended at or before the asserted watermark less the
	// lateness: nothing more can arrive for it, and its rows go.
	BucketExpired Bucket = "expired"
)

// BucketState is what is known about one bucket before it is acted on.
type BucketState struct {
	Bucket Bucket
}

func (b BucketState) String() string { return fmt.Sprintf("bucket=%s", b.Bucket) }

// BucketAction is what happens to a bucket's rows.
type BucketAction string

const (
	// Keep leaves the rows in the table.
	Keep BucketAction = "keep"
	// Publish runs emit_sql over the bucket and hands the result to the sink.
	Publish BucketAction = "publish"
	// Retain leaves the rows for a late row to republish the bucket whole.
	Retain BucketAction = "retain"
	// Purge deletes the rows.
	Purge BucketAction = "purge"
)

// StateOf reduces a poll's readings to the fact the table decides on:
// asserted is the engine's watermark when hasAsserted, closed is this
// window's when hadClosed.
func StateOf(asserted time.Time, hasAsserted bool, closed time.Time, hadClosed bool) State {
	switch {
	case !hasAsserted:
		return State{AssertedNone}
	case hadClosed && !asserted.After(closed):
		return State{AssertedBehind}
	default:
		return State{AssertedAhead}
	}
}

// BucketStateOf reduces one bucket's end to its fact. A bucket that closes
// in this pass is due whatever the lateness; the publish rule says what
// then happens to its rows. Retained and expired are the two fates of a
// bucket that closed in an earlier pass.
func BucketStateOf(decl Declaration, end, closed time.Time, hadClosed bool, asserted time.Time) BucketState {
	switch {
	case end.After(asserted):
		return BucketState{BucketOpen}
	case !hadClosed || end.After(closed):
		return BucketState{BucketDue}
	case !end.Add(decl.Lateness).After(asserted):
		return BucketState{BucketExpired}
	default:
		return BucketState{BucketRetained}
	}
}

// watermarkRule is one row of the watermark table. An empty Asserted matches
// every value. Deciding names the fact that selected the row, and Claim says
// the rule in words.
type watermarkRule struct {
	Name     string
	Asserted []Asserted
	Action   Action
	Deciding string
	Claim    string
}

func (r watermarkRule) matches(s State) bool {
	return len(r.Asserted) == 0 || contains(r.Asserted, s.Asserted)
}

// watermarkTable is the watermark's truth table. checkTables proves that
// the order of its rows does not matter: no state matches two of them.
var watermarkTable = []watermarkRule{
	{
		Name:     "hold.unasserted",
		Asserted: []Asserted{AssertedNone},
		Action:   Hold,
		Deciding: "asserted",
		Claim:    "The engine has asserted no watermark for this window, so nothing is known to be complete and nothing closes.",
	},
	{
		Name:     "hold.behind",
		Asserted: []Asserted{AssertedBehind},
		Action:   Hold,
		Deciding: "asserted",
		Claim:    "The assertion is at or before what this window has already closed, so it has been acted on; the closed watermark never moves backwards.",
	},
	{
		Name:     "follow",
		Asserted: []Asserted{AssertedAhead},
		Action:   Follow,
		Deciding: "asserted",
		Claim:    "The engine has promised that every row it will ever write ends at or after the asserted watermark, so every bucket ending at or before it is complete: the closed watermark follows it, and those buckets publish.",
	},
}

// bucketRule is one row of the bucket table, shaped like watermarkRule.
type bucketRule struct {
	Name     string
	Bucket   []Bucket
	Action   BucketAction
	Deciding string
	Claim    string
}

func (r bucketRule) matches(b BucketState) bool {
	return len(r.Bucket) == 0 || contains(r.Bucket, b.Bucket)
}

// bucketTable is the bucket's truth table.
var bucketTable = []bucketRule{
	{
		Name:     "keep",
		Bucket:   []Bucket{BucketOpen},
		Action:   Keep,
		Deciding: "bucket",
		Claim:    "The bucket ends after the asserted watermark, so its rows stay.",
	},
	{
		Name:     "publish",
		Bucket:   []Bucket{BucketDue},
		Action:   Publish,
		Deciding: "bucket",
		Claim:    "The bucket ends between the closed watermark and the asserted one: emit_sql runs over its rows and the result is published. With allowed_lateness_seconds the rows stay for a late row to republish it whole; without, this pass purges them too.",
	},
	{
		Name:     "retain",
		Bucket:   []Bucket{BucketRetained},
		Action:   Retain,
		Deciding: "bucket",
		Claim:    "The bucket has been published and its lateness has not run out: its rows stay, and a late row the engine admits republishes it whole.",
	},
	{
		Name:     "purge",
		Bucket:   []Bucket{BucketExpired},
		Action:   Purge,
		Deciding: "bucket",
		Claim:    "The bucket ended at or before the asserted watermark less the lateness: nothing more can arrive for it, and its rows are deleted.",
	},
}

// Decide returns the watermark action for a state. checkTables proves at
// load that exactly one row matches every state, so this cannot miss.
func Decide(s State) Action {
	return watermarkRuleFor(s).Action
}

// Next is where an action puts the closed watermark, and whether that moved
// it.
func (a Action) Next(asserted, closed time.Time) (time.Time, bool) {
	if a == Follow {
		return asserted, true
	}
	return closed, false
}

// DecideBucket returns the action for a bucket.
func DecideBucket(b BucketState) BucketAction {
	for _, r := range bucketTable {
		if r.matches(b) {
			return r.Action
		}
	}
	panic(fmt.Sprintf("managers: no rule for %v", b))
}

// watermarkRuleFor is the row a state selects, for the log line a close
// writes.
func watermarkRuleFor(s State) watermarkRule {
	for _, r := range watermarkTable {
		if r.matches(s) {
			return r
		}
	}
	panic(fmt.Sprintf("managers: no rule for %v", s))
}

func contains[T comparable](vals []T, v T) bool {
	for _, x := range vals {
		if x == v {
			return true
		}
	}
	return false
}

// Every value of every fact, for the exhaustiveness check and the rendering.
var (
	assertedValues = []Asserted{AssertedNone, AssertedBehind, AssertedAhead}
	bucketValues   = []Bucket{BucketOpen, BucketDue, BucketRetained, BucketExpired}
)

func allStates() []State {
	var out []State
	for _, a := range assertedValues {
		out = append(out, State{Asserted: a})
	}
	return out
}

func allBucketStates() []BucketState {
	var out []BucketState
	for _, b := range bucketValues {
		out = append(out, BucketState{Bucket: b})
	}
	return out
}

// checkTables proves both tables closed: every combination of facts selects
// exactly one row, and every row is selected by some combination. It runs
// when the package loads, so a broken table fails every test and refuses to
// start.
func checkTables() error {
	reached := map[string]bool{}
	for _, s := range allStates() {
		var names []string
		for _, r := range watermarkTable {
			if r.matches(s) {
				names = append(names, r.Name)
				reached[r.Name] = true
			}
		}
		if err := exactlyOne(s, names); err != nil {
			return fmt.Errorf("watermark table: %w", err)
		}
	}
	for _, r := range watermarkTable {
		if !reached[r.Name] {
			return fmt.Errorf("watermark table: row %s is selected by no combination", r.Name)
		}
	}

	reached = map[string]bool{}
	for _, b := range allBucketStates() {
		var names []string
		for _, r := range bucketTable {
			if r.matches(b) {
				names = append(names, r.Name)
				reached[r.Name] = true
			}
		}
		if err := exactlyOne(b, names); err != nil {
			return fmt.Errorf("bucket table: %w", err)
		}
	}
	for _, r := range bucketTable {
		if !reached[r.Name] {
			return fmt.Errorf("bucket table: row %s is selected by no combination", r.Name)
		}
	}
	return nil
}

func exactlyOne(s fmt.Stringer, names []string) error {
	switch len(names) {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("%v matches no row", s)
	default:
		return fmt.Errorf("%v matches %s", s, strings.Join(names, " and "))
	}
}

func init() {
	if err := checkTables(); err != nil {
		panic("managers: " + err.Error())
	}
}
