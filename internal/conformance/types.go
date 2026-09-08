package conformance

// The type table runner.
//
// Every sink converts Arrow to whatever its destination speaks, and that
// conversion is where five of the seven post-v1.0.4 defects were: an
// undecoded escape (#149), a shifted timestamp (#153), a rejected list
// (#150), a dropped struct field (#151) and a flattened array (#147). None of
// them is visible to a test that writes an int64 and reads it back, which is
// what the resilience harness does.
//
// The declaration says what the sink should do with each type. This runs the
// sink and judges it. A row the two disagree on is a finding: either the sink
// changed and the table is stale, or the table was wrong when it was written.
// It is never a reason to relax the assertion, because the table is what the
// integration page publishes.

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
)

const (
	typeRoundtrip  = "type.roundtrip"
	typeNull       = "type.null"
	typeUndeclared = "type.undeclared.fails_loud"
)

// TypeDestination is one column of one type, and the way back to it.
type TypeDestination struct {
	// Sink writes to a destination holding a single column named "v".
	Sink core.Sink

	// ReadBack returns the one value that column holds, nil when it is null.
	// It must not go through the sink: a destination read through the thing
	// under test cannot contradict it.
	ReadBack func(t *testing.T) (any, error)
}

// TypeSubject is what an integration hands the type runner.
type TypeSubject struct {
	// Integration is the integrations.yml id, e.g. "sink.clickhouse".
	Integration string

	// Declared is the integration's type table. Passed in rather than read
	// here, so the harness's own tests can drive the runner with a table of
	// one row and a known-wrong outcome.
	Declared []coverage.TypeDecl

	// Nulls is what the integration does with a null. It is one statement
	// about every type: ClickHouse stores a null in a non-Nullable column as
	// the column type's zero value, whatever that type is, and asserting that
	// a null survives would assert against the destination's own semantics.
	Nulls coverage.NullRule

	// Prepare creates a destination with a single column "v" of columnType,
	// and a sink writing to it.
	//
	// columnType is empty for an unsupported row: no column type accepts the
	// value, and the batch must fail before any destination matters.
	Prepare func(t *testing.T, key, columnType string) TypeDestination
}

// Types proves the type invariants the subject's table declares.
func Types(t *testing.T, s TypeSubject) {
	t.Helper()
	requireTypeSubject(t, s)

	feature, hasFeature, err := coverage.FeatureFor(s.Integration)
	if err != nil {
		t.Fatalf("conformance: %v", err)
	}

	for _, v := range typeVerdicts(t, s) {
		t.Run(v.invariant, func(t *testing.T) {
			if hasFeature {
				coverage.Covers(t, feature)
			}
			if v.skipped != "" {
				t.Skip(v.skipped)
			}
			if v.failure != "" {
				t.Fatal(v.failure)
			}
			coverage.Invariant(t, v.invariant, s.Integration)
		})
	}
}

func requireTypeSubject(t *testing.T, s TypeSubject) {
	t.Helper()
	if s.Integration == "" {
		t.Fatal("conformance: TypeSubject.Integration is required")
	}
	if s.Prepare == nil {
		t.Fatalf("conformance: %s needs Prepare", s.Integration)
	}
}

// typeVerdicts judges each type invariant across every declared row.
//
// One verdict per invariant rather than per row: the cell is covered only
// when every declared type passed, so the first failing row fails the
// invariant. The failure names the row, because "type.roundtrip failed"
// without a type sends the reader nowhere.
func typeVerdicts(t *testing.T, s TypeSubject) []verdict {
	t.Helper()

	roundtrip := verdict{invariant: typeRoundtrip}
	nulls := verdict{invariant: typeNull}

	for _, d := range s.Declared {
		if f := judgeTypeRow(t, s, d, false); f != "" && roundtrip.failure == "" {
			roundtrip.failure = f
		}
		if f := judgeTypeRow(t, s, d, true); f != "" && nulls.failure == "" {
			nulls.failure = f
		}
	}

	return []verdict{roundtrip, nulls, judgeUndeclaredType(t, s)}
}

// judgeTypeRow writes one value of one type into every column type the row
// names, and judges the declared outcome. It returns the failure, or "".
func judgeTypeRow(t *testing.T, s TypeSubject, d coverage.TypeDecl, null bool) string {
	t.Helper()

	// An unsupported type has no column that accepts it, so it is written
	// once, to no particular column.
	columns := d.Columns
	if d.Outcome == "unsupported" {
		columns = []string{""}
	}

	for _, columnType := range columns {
		arr, err := LatticeArray(d.Key, null)
		if err != nil {
			return fmt.Sprintf("%s: %v", d.Key, err)
		}

		dest := s.Prepare(t, d.Key, columnType)
		tbl := oneColumnTable(arr)

		ctx := context.Background()
		writeErr := dest.Sink.WriteTable(ctx, tbl)
		if writeErr == nil {
			writeErr = dest.Sink.Flush(ctx)
		}
		tbl.Release()
		arr.Release()

		if d.Outcome == "unsupported" {
			if writeErr == nil {
				return fmt.Sprintf("%s is declared unsupported and the sink took it. "+
					"Either the sink gained support and the table is stale, or the "+
					"value reached the destination as some other type", d.Key)
			}
			if got := string(errs.CodeOf(writeErr)); got != d.Code {
				return fmt.Sprintf("%s failed with code %s and the table declares %s: %v",
					d.Key, got, d.Code, writeErr)
			}
			continue
		}

		if writeErr != nil {
			return fmt.Sprintf("%s into %s is declared %s and the sink refused it: %v",
				d.Key, columnType, d.Outcome, writeErr)
		}

		got, err := dest.ReadBack(t)
		if err != nil {
			return fmt.Sprintf("%s into %s: read back: %v", d.Key, columnType, err)
		}
		if null {
			if f := judgeNull(s.Nulls, d.Key, columnType, got); f != "" {
				return f
			}
			continue
		}
		if got == nil {
			return fmt.Sprintf("%s into %s: the value read back as null", d.Key, columnType)
		}
	}
	return ""
}

// judgeNull holds a read-back null against what the integration declared.
//
// Asserting that a null always survives would be asserting against the
// destination. ClickHouse stores a null in a non-Nullable column as the
// column type's zero value and raises nothing, which is a real coercion a
// user must be told about -- not a defect the harness can demand a fix for.
// The claim this invariant makes is that the declared behaviour is the actual
// one, in both directions.
func judgeNull(rule coverage.NullRule, key, columnType string, got any) string {
	switch rule.Outcome {
	case "exact":
		if got != nil {
			return fmt.Sprintf("%s into %s: a null read back as %v, and the table "+
				"declares nulls exact. A Nullable column that is not nullable loses "+
				"the difference between absent and zero", key, columnType, got)
		}
	case "coerced":
		if got == nil {
			return fmt.Sprintf("%s into %s: a null read back as null, and the table "+
				"declares nulls coerced (%s). The destination gained null support "+
				"and the published table now tells users to work around nothing",
				key, columnType, rule.Rule)
		}
	default:
		return fmt.Sprintf("%s: the integration declares no default null outcome, "+
			"so what a null does here is untested and unpublished", key)
	}
	return ""
}

// judgeUndeclaredType writes a type no lattice entry carries. It must fail
// with a code rather than be coerced into whatever the destination accepts.
//
// float16 is the probe: Arrow has it, DuckDB never emits it, so no
// integration declares it and none legitimately supports it.
//
// Both a value and a null are written, and the null is the one that matters.
// A conversion that tests for null before it tests the type lets an all-null
// column of an unsupported type through, and an all-null column is the
// ordinary shape of a field the producer stopped sending.
func judgeUndeclaredType(t *testing.T, s TypeSubject) verdict {
	t.Helper()
	v := verdict{invariant: typeUndeclared}

	for _, probe := range []struct {
		null bool
		what string
	}{
		{false, "a float16 column"},
		{true, "an all-null float16 column"},
	} {
		b := array.NewFloat16Builder(memory.NewGoAllocator())
		if probe.null {
			b.AppendNull()
		} else {
			b.Append(float16.New(1.5))
		}
		arr := b.NewArray()
		b.Release()

		dest := s.Prepare(t, CanonicalKey(arr.DataType()), "")
		tbl := oneColumnTable(arr)

		ctx := context.Background()
		err := dest.Sink.WriteTable(ctx, tbl)
		if err == nil {
			err = dest.Sink.Flush(ctx)
		}
		tbl.Release()
		arr.Release()

		if err == nil {
			v.failure = probe.what + ", which no type table declares, reached the " +
				"destination without an error. An undeclared type must fail the batch " +
				"rather than be coerced into whatever the destination accepts"
			return v
		}
		if errs.CodeOf(err) == errs.CodeInternalUnexpected {
			v.failure = fmt.Sprintf("%s failed with no code, so an operator reads it "+
				"as sqlflow's fault rather than as a column to cast: %v", probe.what, err)
			return v
		}
	}
	return v
}

// oneColumnTable wraps a single array as a one-column table named "v".
func oneColumnTable(arr arrow.Array) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arr.DataType(), Nullable: true},
	}, nil)

	chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	defer chunked.Release()

	col := arrow.NewColumn(schema.Field(0), chunked)
	defer col.Release()

	return array.NewTable(schema, []arrow.Column{*col}, 1)
}
