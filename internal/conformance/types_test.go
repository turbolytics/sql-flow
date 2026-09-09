package conformance

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// The runner gates every sink's type behaviour, so a runner that passes a
// sink which drops a value is worse than no runner. These tests drive it
// against doubles whose behaviour is known, including the two ways a type
// table can lie: claiming support the sink lacks, and claiming a gap the sink
// does not have.

// typeSink converts a fixed set of types and refuses the rest, which is the
// shape of every real sink: a switch with a default arm.
type typeSink struct {
	mu        sync.Mutex
	accept    map[string]bool
	acceptAll bool
	buffered  []arrow.Table
	delivered arrow.Table
}

func newTypeSink(accept ...string) *typeSink {
	s := &typeSink{accept: map[string]bool{}}
	for _, k := range accept {
		s.accept[k] = true
	}
	return s
}

// newPermissiveTypeSink takes anything at all, which is the defect
// type.undeclared.fails_loud exists to catch.
func newPermissiveTypeSink() *typeSink {
	return &typeSink{accept: map[string]bool{}, acceptAll: true}
}

func (s *typeSink) WriteTable(_ context.Context, t arrow.Table) error {
	key := CanonicalKey(t.Schema().Field(0).Type)
	if !s.acceptAll && !s.accept[key] {
		return errs.New(errs.CodeSinkTypeUnsupported, "unsupported arrow type %s", key)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Retain()
	s.buffered = append(s.buffered, t)
	return nil
}

func (s *typeSink) Flush(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.buffered {
		if s.delivered != nil {
			s.delivered.Release()
		}
		s.delivered = t
	}
	s.buffered = nil
	return nil
}

func (s *typeSink) Batch() (arrow.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buffered) == 0 {
		return nil, nil
	}
	return s.buffered[0], nil
}

// value reports whether the delivered row was null, which is all the null
// invariant needs from a double.
func (s *typeSink) value() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered == nil || s.delivered.NumCols() == 0 {
		return nil
	}
	chunk := s.delivered.Column(0).Data().Chunk(0)
	if chunk.IsNull(0) {
		return nil
	}
	// Rendered, not returned raw. A real destination answers with a string, so
	// a double that answers with an Arrow array would slip past the value
	// comparison on a failed type assertion and prove less than it looks.
	if str, ok := chunk.(*array.String); ok {
		return str.Value(0)
	}
	return chunk.ValueStr(0)
}

// lastString returns the single string value of the last delivered table, or
// "" when there is none.
func (s *typeSink) lastString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered == nil || s.delivered.NumCols() == 0 {
		return ""
	}
	str, ok := s.delivered.Column(0).Data().Chunk(0).(*array.String)
	if !ok || str.IsNull(0) {
		return ""
	}
	return str.Value(0)
}

// secondElementIsNull reports whether the second element of the delivered
// list row is null, which is the question a real destination answers with a
// query rather than by inspecting Arrow.
func (s *typeSink) secondElementIsNull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered == nil || s.delivered.NumCols() == 0 {
		return false
	}
	list, ok := s.delivered.Column(0).Data().Chunk(0).(*array.List)
	if !ok {
		return false
	}
	values := list.ListValues()
	if values.Len() < 2 {
		return false
	}
	return values.IsNull(1)
}

func typeSubject(s *typeSink, declared []coverage.TypeDecl) TypeSubject {
	return TypeSubject{
		Integration: "sink.conformance_double",
		Declared:    declared,
		Nulls:       coverage.NullRule{Outcome: "exact"},
		Prepare: func(t *testing.T, key, columnType string) TypeDestination {
			return TypeDestination{
				Sink:     core.Sink(s),
				ReadBack: func(t *testing.T) (any, error) { return s.value(), nil },
				// The double keeps what it is given, so its second element is
				// null exactly when the written one was.
				ReadBackNullElement: func(t *testing.T) (bool, error) {
					return s.secondElementIsNull(), nil
				},
			}
		},
	}
}

func TestToolingConformanceTypes_ADoubleThatHonoursItsTablePasses(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	// A container row and a string row are both part of honouring the table:
	// type.nested and type.string.fidelity each refuse to pass on a table that
	// declares nothing they can exercise.
	// The expectations are the canonical values internal/conformance/lattice.go
	// writes, rendered the way the double answers. int64 writes its type's
	// minimum, so a value stored one width too narrow wraps and is caught here.
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64", Expect: "-9223372036854775808"}}},
		// The second entry is #153's shape: text bound for a temporal column,
		// which no Arrow key describes. The double keeps what it is given, so
		// the text it stores is the text it took whatever zone the host is in.
		{Key: "utf8", Outcome: "exact", Columns: []coverage.ColumnDecl{
			{Type: "String", Expect: "L'Œil 👁 \"quoted\" back\\slash\ttab"},
			{Type: "DateTime64(3)", Value: "2026-09-01 12:00:00.123",
				Expect: "2026-09-01 12:00:00.123", Instant: true},
		}},
		{Key: "list<int64>", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Array(Int64)", Expect: "[-9223372036854775808,-9223372036854775808,-9223372036854775808]"}}},
		// Arrow's own rendering, in UTC. It is the same string whether the host
		// is in UTC or nine hours ahead, which is the property
		// type.timestamp.instant asks for.
		{Key: "timestamp[us]", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "DateTime64(6)", Expect: "2026-09-08T12:00:00.123456Z"}}},
	}
	s := typeSubject(newTypeSink("int64", "utf8", "list<int64>", "timestamp[us]"), declared)
	// The double keeps every value it is given, nulls included.
	s.ListElementNulls = coverage.NullRule{Outcome: "exact"}

	Types(t, s)
}

// The defect the runner exists to catch: a table claiming exact for a type
// the sink refuses. On the published page it tells a user a cast will work.
func TestToolingConformanceTypes_ATypeDeclaredExactThatFailsIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64", Expect: "v"}}},
	}
	v := typeVerdicts(t, typeSubject(newTypeSink(), declared))
	assertTypeFailure(t, v, typeRoundtrip, "int64")
}

// The mirror defect: a table claiming unsupported for a type that works. It
// hides working support, and tells a user to cast a column they need not.
func TestToolingConformanceTypes_ATypeDeclaredUnsupportedThatWorksIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "unsupported", Code: "user.sink.type_unsupported"},
	}
	v := typeVerdicts(t, typeSubject(newTypeSink("int64"), declared))
	assertTypeFailure(t, v, typeRoundtrip, "int64")
}

// An unsupported type must fail with the declared code. A bare error reaches
// the operator as system.internal.unexpected, which sends them to file a bug
// rather than to cast the column.
func TestToolingConformanceTypes_AFailureWithTheWrongCodeIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "unsupported", Code: "user.sink.some_other_code"},
	}
	// The sink refuses int64 with user.sink.type_unsupported; the table names
	// a different code, so the row is unproven even though both agree it fails.
	v := typeVerdicts(t, typeSubject(newTypeSink(), declared))
	assertTypeFailure(t, v, typeRoundtrip, "user.sink.some_other_code")
}

// A type absent from the table must fail the batch rather than be coerced
// into whatever the destination happens to accept.
func TestToolingConformanceTypes_AnUndeclaredTypeThatSucceedsIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64", Expect: "v"}}},
	}
	v := typeVerdicts(t, typeSubject(newPermissiveTypeSink(), declared))
	assertTypeFailure(t, v, typeUndeclared, "float16")
}

// A null must survive as a null. A sink that turns one into a value is the
// silent kind of wrong, so the runner judges it separately from the value.
func TestToolingConformanceTypes_ANullReadBackAsAValueIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64", Expect: "v"}}},
	}
	s := newTypeSink("int64")
	subject := typeSubject(s, declared)
	subject.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		return TypeDestination{
			Sink: core.Sink(s),
			// A destination that reports a value whatever it was given, which
			// is what a non-Nullable column does with a null.
			ReadBack: func(t *testing.T) (any, error) { return int64(0), nil },
		}
	}
	assertTypeFailure(t, typeVerdicts(t, subject), typeNull, "int64")
}

// The mirror case: a table declaring nulls coerced, against a destination
// that keeps them. Nothing is broken, and the published page still tells
// users to work around a coercion that no longer happens.
func TestToolingConformanceTypes_ANullDeclaredCoercedThatSurvivesIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64", Expect: "v"}}},
	}
	subject := typeSubject(newTypeSink("int64"), declared)
	subject.Nulls = coverage.NullRule{Outcome: "coerced", Rule: "the zero value"}

	assertTypeFailure(t, typeVerdicts(t, subject), typeNull, "int64")
}

// An integration that says nothing about nulls has an untested and
// unpublished behaviour, which is worse than either declared outcome.
func TestToolingConformanceTypes_ANullWithNoDeclaredOutcomeIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64", Expect: "v"}}},
	}
	subject := typeSubject(newTypeSink("int64"), declared)
	subject.Nulls = coverage.NullRule{}

	assertTypeFailure(t, typeVerdicts(t, subject), typeNull, "int64")
}

// A container that takes an element the sink cannot convert has laundered it:
// the row lands, the column reads back, and one value in it is a fiction.
// This is what type.nested means by "never silently flattened".
func TestToolingConformanceTypes_AnUnsupportedElementInsideAListIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "list<struct>", Outcome: "unsupported", Code: "user.sink.type_unsupported"},
	}
	// The double takes the list despite its element, which is the defect.
	s := typeSubject(newTypeSink("list<struct>"), declared)
	s.ListElementNulls = coverage.NullRule{Outcome: "coerced", Rule: "the element zero value"}

	assertTypeFailure(t, typeVerdicts(t, s), typeNested, "list<struct>")
}

// A null inside a list has no error path. The sink either keeps it, drops it,
// or substitutes a value, and all three look identical from outside.
func TestToolingConformanceTypes_ANullElementAgainstTheWrongRuleIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "list<int64>", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Array(Int64)", Expect: "v"}}},
	}
	s := typeSubject(newTypeSink("list<int64>"), declared)
	// The double keeps the null it is given, so a table claiming the element
	// is coerced to a zero value is wrong about it -- and a published page
	// would be warning readers about a coercion that does not happen.
	s.ListElementNulls = coverage.NullRule{Outcome: "coerced", Rule: "the element zero value"}

	assertTypeFailure(t, typeVerdicts(t, s), typeNested, "list<int64>")
}

// A subject with no container rows cannot prove the invariant, and must say so
// rather than pass vacuously.
func TestToolingConformanceTypes_ATableWithNoContainerRowsIsNotNestedProof(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64", Expect: "v"}}},
	}
	s := typeSubject(newTypeSink("int64"), declared)

	assertTypeFailure(t, typeVerdicts(t, s), typeNested, "no container")
}

// #149's defect: jsonparser returns the bytes between a string's quotes with
// escapes undecoded, and the sink stored them verbatim. Every affected value
// round-tripped through tests that used one tidy string.
func TestToolingConformanceTypes_AMangledStringIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "utf8", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "String", Expect: "v"}}},
	}
	s := typeSubject(newTypeSink("utf8"), declared)
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("utf8")
		return TypeDestination{
			Sink: core.Sink(sink),
			// A destination that drops backslashes, which is the shape of the
			// defect: no error, and the row is simply wrong.
			ReadBack: func(t *testing.T) (any, error) {
				return strings.ReplaceAll(sink.lastString(), "\\", ""), nil
			},
		}
	}

	assertTypeFailure(t, typeVerdicts(t, s), typeFidelity, "backslash")
}

// The empty string is a value, not an absence. A destination that conflates
// them loses the difference between sending nothing and sending no characters.
func TestToolingConformanceTypes_AnEmptyStringReadBackAsNullIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "utf8", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "String", Expect: "v"}}},
	}
	s := typeSubject(newTypeSink("utf8"), declared)
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("utf8")
		return TypeDestination{
			Sink: core.Sink(sink),
			ReadBack: func(t *testing.T) (any, error) {
				if sink.lastString() == "" {
					return nil, nil
				}
				return sink.lastString(), nil
			},
		}
	}

	assertTypeFailure(t, typeVerdicts(t, s), typeFidelity, "empty")
}

// An integration with no string row cannot prove the claim and must say so.
func TestToolingConformanceTypes_ATableWithNoStringRowIsNotFidelityProof(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64", Expect: "v"}}},
		{Key: "list<int64>", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Array(Int64)", Expect: "v"}}},
	}
	s := typeSubject(newTypeSink("int64", "list<int64>"), declared)
	s.ListElementNulls = coverage.NullRule{Outcome: "exact"}

	assertTypeFailure(t, typeVerdicts(t, s), typeFidelity, "no utf8 row")
}

// The hole this closes: a sink that takes a value, stores something else, and
// returns it without an error passed type.roundtrip for as long as the
// read-back was not nil.
func TestToolingConformanceTypes_AValueChangedInFlightIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "utf8", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "String", Expect: "hello"}}},
	}
	s := typeSubject(newTypeSink("utf8"), declared)
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("utf8")
		return TypeDestination{
			Sink: core.Sink(sink),
			// A destination that stores something other than what it took.
			ReadBack: func(t *testing.T) (any, error) { return "something else", nil },
		}
	}

	assertTypeFailure(t, typeVerdicts(t, s), typeRoundtrip, "something else")
}

// A row claiming a value survives and declaring no expectation cannot be
// judged, and must say so rather than pass.
func TestToolingConformanceTypes_AnExactRowWithNoExpectationIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64"}}},
	}
	s := typeSubject(newTypeSink("int64"), declared)

	assertTypeFailure(t, typeVerdicts(t, s), typeRoundtrip, "no expect")
}

// An unsupported row needs no expectation: it never reaches a destination.
func TestToolingConformanceTypes_AnUnsupportedRowNeedsNoExpectation(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "unsupported", Code: "user.sink.type_unsupported"},
	}
	for _, entry := range typeVerdicts(t, typeSubject(newTypeSink(), declared)) {
		if entry.invariant == typeRoundtrip && entry.failure != "" {
			t.Fatalf("type.roundtrip failed on an unsupported row: %s", entry.failure)
		}
	}
}

// #153's defect: clickhouse-go parses a zone-less string in time.Local, so the
// stored value depended on the host's offset. A test on a UTC laptop saw
// nothing wrong, which is why the runner moves the host zone before it writes.
func TestToolingConformanceTypes_AHostZoneLeakIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "timestamp[us]", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "DateTime64(6)", Expect: "2026-09-08 12:00:00.123456"}}},
		{Key: "utf8", Outcome: "exact", Columns: []coverage.ColumnDecl{
			{Type: "DateTime64(3)", Value: "2026-09-01 12:00:00.123",
				Expect: "2026-09-01 12:00:00.123", Instant: true},
		}},
	}
	s := typeSubject(newTypeSink("timestamp[us]", "utf8"), declared)
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("timestamp[us]", "utf8")
		return TypeDestination{
			Sink: core.Sink(sink),
			// A destination that renders in whatever zone the host is in,
			// which is the shape of a leak.
			ReadBack: func(t *testing.T) (any, error) {
				return "2026-09-08 21:00:00.123456", nil
			},
		}
	}

	assertTypeFailure(t, typeVerdicts(t, s), typeInstant, "2026-09-08 21:00:00.123456")
}

// A table with no temporal row cannot prove the claim and must say so.
func TestToolingConformanceTypes_ATableWithNoTemporalRowIsNotInstantProof(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "Int64", Expect: "-128"}}},
	}
	assertTypeFailure(t, typeVerdicts(t, typeSubject(newTypeSink("int64"), declared)),
		typeInstant, "no temporal row")
}

// #153 itself: a zone-less string stored shifted by the host's offset. The
// probe writes nine hours ahead of UTC, so the old behaviour lands at 21:00.
func TestToolingConformanceTypes_ATemporalStringShiftedByTheHostIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "timestamp[us]", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "DateTime64(6)", Expect: "2026-09-08T12:00:00.123456Z"}}},
		{Key: "utf8", Outcome: "exact", Columns: []coverage.ColumnDecl{
			{Type: "DateTime64(3)", Value: "2026-09-01 12:00:00.123",
				Expect: "2026-09-01 12:00:00.123", Instant: true},
		}},
	}
	s := typeSubject(newTypeSink("timestamp[us]", "utf8"), declared)
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("timestamp[us]", "utf8")
		return TypeDestination{
			Sink: core.Sink(sink),
			ReadBack: func(t *testing.T) (any, error) {
				if key == "utf8" {
					return "2026-09-01 21:00:00.123", nil
				}
				return "2026-09-08T12:00:00.123456Z", nil
			},
		}
	}

	assertTypeFailure(t, typeVerdicts(t, s), typeInstant, "21:00:00.123")
}

// A table that marks no pair instant leaves #153's shape unproven, and the
// cell would claim a regression it never wrote.
func TestToolingConformanceTypes_ANoInstantPairIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "timestamp[us]", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "DateTime64(6)", Expect: "2026-09-08T12:00:00.123456Z"}}},
	}
	s := typeSubject(newTypeSink("timestamp[us]"), declared)

	assertTypeFailure(t, typeVerdicts(t, s), typeInstant, "instant")
}

// The runner must put the host zone back. A verdict that leaves the process
// nine hours from UTC corrupts every test that runs after it.
func TestToolingConformanceTypes_TheHostZoneIsRestored(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	before := time.Local
	declared := []coverage.TypeDecl{
		{Key: "timestamp[us]", Outcome: "exact", Columns: []coverage.ColumnDecl{{Type: "DateTime64(6)", Expect: "2026-09-08 12:00:00.123456"}}},
	}
	typeVerdicts(t, typeSubject(newTypeSink("timestamp[us]"), declared))

	if time.Local != before {
		t.Fatalf("time.Local is %v, and it was %v before the run", time.Local, before)
	}
}

// A marker carries an integration id, so a subject without one would emit
// evidence that lands on no cell.
func TestToolingConformanceTypes_ASubjectWithoutAnIntegrationIdIsRejected(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	s := typeSubject(newTypeSink(), nil)
	s.Integration = ""

	assert.True(t, fatals(t, func(t *testing.T) { requireTypeSubject(t, s) }))
}

func TestToolingConformanceTypes_ASubjectMissingPrepareIsRejected(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	s := typeSubject(newTypeSink(), nil)
	s.Prepare = nil

	assert.True(t, fatals(t, func(t *testing.T) { requireTypeSubject(t, s) }))
}

func assertTypeFailure(t *testing.T, verdicts []verdict, invariant, substring string) {
	t.Helper()
	for _, v := range verdicts {
		if v.invariant != invariant {
			continue
		}
		if v.failure == "" {
			t.Fatalf("%s passed; it must fail", invariant)
		}
		if substring != "" && !strings.Contains(v.failure, substring) {
			t.Fatalf("%s failure %q does not name %q", invariant, v.failure, substring)
		}
		return
	}
	t.Fatalf("no verdict for %s", invariant)
}
