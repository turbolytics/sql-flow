package conformance

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
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
	return chunk
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
			}
		},
	}
}

func TestToolingConformanceTypes_ADoubleThatHonoursItsTablePasses(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	Types(t, typeSubject(newTypeSink("int64"), declared))
}

// The defect the runner exists to catch: a table claiming exact for a type
// the sink refuses. On the published page it tells a user a cast will work.
func TestToolingConformanceTypes_ATypeDeclaredExactThatFailsIsCaught(t *testing.T) {
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	v := typeVerdicts(t, typeSubject(newTypeSink(), declared))
	assertTypeFailure(t, v, typeRoundtrip, "int64")
}

// The mirror defect: a table claiming unsupported for a type that works. It
// hides working support, and tells a user to cast a column they need not.
func TestToolingConformanceTypes_ATypeDeclaredUnsupportedThatWorksIsCaught(t *testing.T) {
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
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	v := typeVerdicts(t, typeSubject(newPermissiveTypeSink(), declared))
	assertTypeFailure(t, v, typeUndeclared, "float16")
}

// A null must survive as a null. A sink that turns one into a value is the
// silent kind of wrong, so the runner judges it separately from the value.
func TestToolingConformanceTypes_ANullReadBackAsAValueIsCaught(t *testing.T) {
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
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
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	subject := typeSubject(newTypeSink("int64"), declared)
	subject.Nulls = coverage.NullRule{Outcome: "coerced", Rule: "the zero value"}

	assertTypeFailure(t, typeVerdicts(t, subject), typeNull, "int64")
}

// An integration that says nothing about nulls has an untested and
// unpublished behaviour, which is worse than either declared outcome.
func TestToolingConformanceTypes_ANullWithNoDeclaredOutcomeIsCaught(t *testing.T) {
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	subject := typeSubject(newTypeSink("int64"), declared)
	subject.Nulls = coverage.NullRule{}

	assertTypeFailure(t, typeVerdicts(t, subject), typeNull, "int64")
}

// A marker carries an integration id, so a subject without one would emit
// evidence that lands on no cell.
func TestToolingConformanceTypes_ASubjectWithoutAnIntegrationIdIsRejected(t *testing.T) {
	s := typeSubject(newTypeSink(), nil)
	s.Integration = ""

	assert.True(t, fatals(t, func(t *testing.T) { requireTypeSubject(t, s) }))
}

func TestToolingConformanceTypes_ASubjectMissingPrepareIsRejected(t *testing.T) {
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
