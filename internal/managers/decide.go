package managers

import (
	"fmt"
	"strings"
	"time"
)

// Every operation a window performs is a row in one of two tables here.
//
// A poll builds a State from what it read, hands it to Decide, and performs
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
// whole of what a poll decides on.

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

// Bucket is where one bucket stands against the previous closed watermark
// and the one this poll decided.
type Bucket string

const (
	// BucketLate is a bucket that ends at or before the previous watermark:
	// its rows arrived after it closed.
	BucketLate Bucket = "late"
	// BucketDue is a bucket that ends after the previous watermark and at or
	// before the next.
	BucketDue Bucket = "due"
	// BucketOpen is a bucket that ends after the next watermark.
	BucketOpen Bucket = "open"
)

// BucketState is what is known about one bucket before it is acted on.
type BucketState struct {
	Bucket Bucket
	Policy LatePolicy
}

func (b BucketState) String() string { return fmt.Sprintf("bucket=%s policy=%s", b.Bucket, b.Policy) }

// BucketAction is what happens to a bucket's rows.
type BucketAction string

const (
	// Keep leaves the rows in the table.
	Keep BucketAction = "keep"
	// Close publishes emit_sql over the bucket and deletes its rows.
	Close BucketAction = "close"
	// DropLate deletes the rows and counts them as late.
	DropLate BucketAction = "late.drop"
	// ReemitLate publishes emit_sql over the late rows alone, deletes them
	// and counts them as late.
	ReemitLate BucketAction = "late.reemit"
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

// BucketStateOf reduces one bucket's end to its fact.
func BucketStateOf(decl Declaration, end, previous, next time.Time) BucketState {
	b := BucketState{Policy: decl.Late}
	switch {
	case !end.After(previous):
		b.Bucket = BucketLate
	case !end.After(next):
		b.Bucket = BucketDue
	default:
		b.Bucket = BucketOpen
	}
	return b
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
	Policy   []LatePolicy
	Action   BucketAction
	Deciding string
	Claim    string
}

func (r bucketRule) matches(b BucketState) bool {
	return (len(r.Bucket) == 0 || contains(r.Bucket, b.Bucket)) &&
		(len(r.Policy) == 0 || contains(r.Policy, b.Policy))
}

// bucketTable is the bucket's truth table.
var bucketTable = []bucketRule{
	{
		Name:     "keep",
		Bucket:   []Bucket{BucketOpen},
		Action:   Keep,
		Deciding: "bucket",
		Claim:    "The bucket ends after the watermark, so its rows stay.",
	},
	{
		Name:     "close",
		Bucket:   []Bucket{BucketDue},
		Action:   Close,
		Deciding: "bucket",
		Claim:    "The bucket ends between the previous watermark and this one: emit_sql runs over its rows and they are deleted.",
	},
	{
		Name:     "late.drop",
		Bucket:   []Bucket{BucketLate},
		Policy:   []LatePolicy{LateDrop},
		Action:   DropLate,
		Deciding: "policy",
		Claim:    "The bucket closed before these rows arrived, and late_rows is drop: they are deleted and counted.",
	},
	{
		Name:     "late.reemit",
		Bucket:   []Bucket{BucketLate},
		Policy:   []LatePolicy{LateReemit},
		Action:   ReemitLate,
		Deciding: "policy",
		Claim:    "The bucket closed before these rows arrived, and late_rows is reemit: emit_sql runs over the late rows alone, they are deleted and counted.",
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
	bucketValues   = []Bucket{BucketLate, BucketDue, BucketOpen}
	policyValues   = []LatePolicy{LateDrop, LateReemit}
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
		for _, p := range policyValues {
			out = append(out, BucketState{Bucket: b, Policy: p})
		}
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
