package managers

import (
	"fmt"
	"strings"
	"time"
)

// Every operation a window performs is a row in one of two tables here.
//
// A poll builds a State from what it read, hands it to Decide, and performs
// the Action it gets back. The tables are the only place a combination of
// facts meets an action, so the logic is read in one place, every
// combination is accounted for, and checkTables proves it at load: each
// combination selects exactly one row, and every row is reachable. The
// Markdown in docs/windows/decisions.md is rendered from these tables by a
// test, so what is published is what runs.

// Data is where the window's rows stand against the committed watermark, in
// event time.
type Data string

const (
	// DataNone is a window with no rows.
	DataNone Data = "none"
	// DataBehind is a newest bucket that ends at or before the watermark:
	// everything the window holds has already closed.
	DataBehind Data = "behind"
	// DataOpen is a newest bucket that ends after the watermark while its
	// start less the grace does not pass it: something is open, and the
	// stream has not moved far enough past it to close it.
	DataOpen Data = "open"
	// DataRipe is a newest bucket whose start less the grace passes the
	// watermark, or any rows at all before the first close.
	DataRipe Data = "ripe"
)

// Idle is what the engine's progress row confirms about the stream.
type Idle string

const (
	// IdleOff is a window with no idle_close_seconds.
	IdleOff Idle = "off"
	// IdleUnconfirmed is a row that confirms less quiet than the bound,
	// including a row the engine has not written yet.
	IdleUnconfirmed Idle = "unconfirmed"
	// IdleConfirmed is a commit made at least the bound after the newest
	// arrival.
	IdleConfirmed Idle = "confirmed"
)

// Source is whether the engine could have received anything at all.
type Source string

const (
	// SourceDelivering is a source holding what it needs to deliver: a
	// consumer with partitions, a connected websocket.
	SourceDelivering Source = "delivering"
	// SourceNotDelivering is a source that holds nothing: a consumer between
	// assignments, a websocket reconnecting. Time it could not deliver is not
	// quiet, so no bucket closes on idleness across it.
	SourceNotDelivering Source = "not_delivering"
)

// State is what one poll knows before it decides.
type State struct {
	Data   Data
	Idle   Idle
	Source Source
}

func (s State) String() string {
	return fmt.Sprintf("data=%s idle=%s source=%s", s.Data, s.Idle, s.Source)
}

// Action is what a poll does with the watermark.
type Action string

const (
	// Hold leaves the watermark where it is.
	Hold Action = "hold"
	// CloseByGrace moves the watermark to the newest bucket's start less the
	// grace.
	CloseByGrace Action = "close.grace"
	// CloseByIdle moves the watermark to the newest bucket's end.
	CloseByIdle Action = "close.idle"
)

// Bucket is where one bucket stands against the previous watermark and the
// one this poll decided.
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

// StateOf reduces a poll's readings to the facts the table decides on.
//
// newest is the newest bucket's start when hasRows; previous is the committed
// watermark when hadPrevious; quiet is what the progress row confirms.
func StateOf(decl Declaration, newest time.Time, hasRows bool, previous time.Time,
	hadPrevious bool, quiet, deliveringFor time.Duration, delivering bool) State {
	s := State{Idle: IdleOff, Source: SourceDelivering}
	if !delivering {
		s.Source = SourceNotDelivering
	}
	if decl.IdleClose > 0 {
		// The engine can only confirm quiet it could have heard: a source
		// that resumed 30s ago has confirmed 30s at most, whatever the last
		// arrival says. A negative deliveringFor is a source that never said,
		// and bounds nothing.
		//
		// What a source holding nothing means is the table's to say, not
		// this function's. Zeroing the quiet here instead said it twice: the
		// idle fact came out unconfirmed, hold.not_delivering became a row no
		// reading could select, and the table was left claiming that a ripe
		// bucket holds through an outage — which is wrong, and which only
		// the zeroing prevented.
		if deliveringFor >= 0 && deliveringFor < quiet {
			quiet = deliveringFor
		}
		s.Idle = IdleUnconfirmed
		if quiet >= decl.IdleClose {
			s.Idle = IdleConfirmed
		}
	}
	switch {
	case !hasRows:
		s.Data = DataNone
	case !hadPrevious, newest.Add(-decl.Grace).After(previous):
		s.Data = DataRipe
	case !newest.Add(decl.Size).After(previous):
		s.Data = DataBehind
	default:
		s.Data = DataOpen
	}
	return s
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

// watermarkRule is one row of the watermark table. An empty Data or Idle
// matches every value of that fact. Deciding names the fact that selected
// the row, and Claim says the rule in words.
type watermarkRule struct {
	Name     string
	Data     []Data
	Idle     []Idle
	Source   []Source
	Action   Action
	Deciding string
	Claim    string
}

func (r watermarkRule) matches(s State) bool {
	return (len(r.Data) == 0 || contains(r.Data, s.Data)) &&
		(len(r.Idle) == 0 || contains(r.Idle, s.Idle)) &&
		(len(r.Source) == 0 || contains(r.Source, s.Source))
}

// watermarkTable is the watermark's truth table. checkTables proves that
// the order of its rows does not matter: no state matches two of them.
var watermarkTable = []watermarkRule{
	{
		Name:     "hold.empty",
		Data:     []Data{DataNone},
		Action:   Hold,
		Deciding: "data",
		Claim:    "The window holds no rows, so there is nothing to close.",
	},
	{
		Name:     "hold.behind",
		Data:     []Data{DataBehind},
		Action:   Hold,
		Deciding: "data",
		Claim:    "Everything the window holds ended at or before the watermark, so it has already closed; the watermark never moves backwards.",
	},
	{
		Name:     "hold.open",
		Data:     []Data{DataOpen},
		Idle:     []Idle{IdleOff, IdleUnconfirmed},
		Action:   Hold,
		Deciding: "idle",
		Claim:    "A bucket is open, the stream has not moved past it by the grace, and the engine has not confirmed the stream quiet.",
	},
	{
		Name:     "hold.not_delivering",
		Data:     []Data{DataOpen},
		Idle:     []Idle{IdleConfirmed},
		Source:   []Source{SourceNotDelivering},
		Action:   Hold,
		Deciding: "source",
		Claim:    "The source could not deliver, so silence says nothing about the stream and no bucket closes on idleness across it.",
	},
	{
		Name:     "close.grace.not_delivering",
		Data:     []Data{DataRipe},
		Idle:     []Idle{IdleConfirmed},
		Source:   []Source{SourceNotDelivering},
		Action:   CloseByGrace,
		Deciding: "data",
		Claim:    "The stream itself moved past the watermark by the grace, which is evidence the data carries and no source has to confirm, so the bucket closes by the grace however long the source held nothing.",
	},
	{
		Name:     "close.idle",
		Data:     []Data{DataOpen, DataRipe},
		Idle:     []Idle{IdleConfirmed},
		Source:   []Source{SourceDelivering},
		Action:   CloseByIdle,
		Deciding: "idle",
		Claim:    "The engine committed idle_close_seconds after the newest arrival with nothing else arriving, so every open bucket closes, up to the newest bucket's end.",
	},
	{
		Name:     "close.grace",
		Data:     []Data{DataRipe},
		Idle:     []Idle{IdleOff, IdleUnconfirmed},
		Action:   CloseByGrace,
		Deciding: "data",
		Claim:    "The stream has moved past the watermark by the grace, so the watermark follows it to the newest bucket's start less the grace.",
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

// Next is where an action puts the watermark, and whether that moved it.
func (a Action) Next(decl Declaration, newest, previous time.Time) (time.Time, bool) {
	switch a {
	case CloseByGrace:
		return newest.Add(-decl.Grace), true
	case CloseByIdle:
		return newest.Add(decl.Size), true
	default:
		return previous, false
	}
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
	dataValues   = []Data{DataNone, DataBehind, DataOpen, DataRipe}
	idleValues   = []Idle{IdleOff, IdleUnconfirmed, IdleConfirmed}
	sourceValues = []Source{SourceDelivering, SourceNotDelivering}
	bucketValues = []Bucket{BucketLate, BucketDue, BucketOpen}
	policyValues = []LatePolicy{LateDrop, LateReemit}
)

func allStates() []State {
	var out []State
	for _, d := range dataValues {
		for _, i := range idleValues {
			for _, src := range sourceValues {
				out = append(out, State{Data: d, Idle: i, Source: src})
			}
		}
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
