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
	"strings"
	"testing"
	"time"

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
	typeNested     = "type.nested"
	typeInstant    = "type.timestamp.instant"
	typeFidelity   = "type.string.fidelity"
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

	// ReadBackNullElement reports whether the second element of the single
	// list row is null.
	//
	// It exists because nil-ness cannot answer the question. A list holding a
	// null element is not itself null, so ReadBack returns a value either way
	// and [1, 0, 3] is indistinguishable from [1, NULL, 3] through it. Only
	// the destination can say which it holds.
	//
	// Required of a subject whose table declares any list row.
	ReadBackNullElement func(t *testing.T) (bool, error)
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

	// ListElementNulls is what the integration does with a null held inside a
	// non-null list. It is a separate statement from Nulls because the answer
	// differs: a ClickHouse column can be Nullable and its Array(T) elements
	// cannot, so the column-level rule says nothing about what a list holds.
	ListElementNulls coverage.NullRule

	// TemporalString is a timestamp that reaches the sink as text, bound for a
	// temporal column. No Arrow type describes that pairing, and it is the
	// exact shape of #153, so a table keyed on Arrow types alone never writes
	// it.
	TemporalString coverage.TemporalString

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

	return []verdict{roundtrip, nulls, judgeNested(t, s),
		judgeTimestampInstant(t, s), judgeStringFidelity(t, s),
		judgeUndeclaredType(t, s)}
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
		// A non-null read-back is not proof the value survived. A sink that
		// takes a value, stores something else and returns it without an
		// error satisfies everything above.
		want := d.Expect
		if override, ok := d.ExpectPerColumn[columnType]; ok {
			want = override
		}
		if want == "" {
			return fmt.Sprintf("%s into %s is declared %s and the table names no "+
				"expect, so nothing checks what the destination holds. A row that "+
				"claims a value survives must say what survives",
				d.Key, columnType, d.Outcome)
		}
		if rendered, ok := got.(string); ok && rendered != want {
			return fmt.Sprintf("%s into %s read back as %q, and the table expects "+
				"%q. Either the sink changed what it stores, or the table was "+
				"wrong when it was written", d.Key, columnType, rendered, want)
		}
	}
	return ""
}

// judgeNested proves that a container carries what it holds, and hides
// nothing.
//
// type.roundtrip already writes every declared row, containers included, so
// this adds the two claims that row cannot make. First, a table declaring no
// container at all is not proof: an integration that never writes a list has
// not shown it handles one, and a vacuous pass is worse than a missing cell.
// Second, a null inside a non-null list behaves as declared -- the position
// with no error path, where a sink can keep, drop or substitute and all three
// look the same from outside.
func judgeNested(t *testing.T, s TypeSubject) verdict {
	t.Helper()
	v := verdict{invariant: typeNested}

	var lists []coverage.TypeDecl
	containers := 0
	for _, d := range s.Declared {
		switch {
		case strings.HasPrefix(d.Key, "list<"):
			containers++
			lists = append(lists, d)
		case strings.HasPrefix(d.Key, "fixed_size_list<"),
			d.Key == "struct", d.Key == "map", d.Key == "dictionary",
			d.Key == "sparse_union":
			containers++
		}
	}
	if containers == 0 {
		v.failure = "the type table declares no container row, so nothing here " +
			"proves what happens to a list, a struct or a map. An integration " +
			"that never writes one has not shown it handles one"
		return v
	}

	for _, d := range lists {
		if f := judgeListElementNull(t, s, d); f != "" {
			v.failure = f
			return v
		}
	}
	return v
}

// judgeListElementNull writes [value, null, value] and holds the read-back
// against the declared rule.
func judgeListElementNull(t *testing.T, s TypeSubject, d coverage.TypeDecl) string {
	t.Helper()

	// An unsupported element has no column that accepts it, so it is written
	// once, to no particular column.
	columns := d.Columns
	if d.Outcome == "unsupported" {
		columns = []string{""}
	}

	for _, columnType := range columns {
		arr, err := LatticeListWithNullElement(d.Key)
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

		// An unsupported element must fail from inside the container exactly
		// as it does alone. A container that takes it has laundered it.
		if d.Outcome == "unsupported" {
			if writeErr == nil {
				return fmt.Sprintf("%s holds an element the table declares "+
					"unsupported, and the sink took the list anyway. The value "+
					"reached the destination as something else, which is the "+
					"silent flattening this invariant forbids", d.Key)
			}
			continue
		}

		if writeErr != nil {
			return fmt.Sprintf("%s into %s is declared %s and the sink refused a "+
				"list holding a null element: %v", d.Key, columnType, d.Outcome, writeErr)
		}

		got, err := dest.ReadBack(t)
		if err != nil {
			return fmt.Sprintf("%s into %s: read back: %v", d.Key, columnType, err)
		}
		if got == nil {
			return fmt.Sprintf("%s into %s: a list holding one null read back as "+
				"null in its entirety. One absent element must not erase the row",
				d.Key, columnType)
		}

		if dest.ReadBackNullElement == nil {
			return fmt.Sprintf("%s is declared and the subject supplies no "+
				"ReadBackNullElement, so what the destination did with the null "+
				"element is unverified. The declaration would be prose", d.Key)
		}
		isNull, err := dest.ReadBackNullElement(t)
		if err != nil {
			return fmt.Sprintf("%s into %s: read back the null element: %v",
				d.Key, columnType, err)
		}
		if f := judgeElementNull(s.ListElementNulls, d.Key, columnType, isNull); f != "" {
			return f
		}
	}
	return ""
}

// judgeElementNull holds what the destination did with a null list element
// against what the integration declared.
//
// Separate from judgeNull because the evidence is different. A null column is
// judged by nil-ness; a null element cannot be, because the list holding it is
// not null. The destination answers directly instead.
func judgeElementNull(rule coverage.NullRule, key, columnType string, isNull bool) string {
	switch rule.Outcome {
	case "exact":
		if !isNull {
			return fmt.Sprintf("%s into %s: a null element read back as a value, "+
				"and the table declares list elements exact. The difference between "+
				"an absent element and a zero one is gone, with no error to say so",
				key, columnType)
		}
	case "coerced":
		if isNull {
			return fmt.Sprintf("%s into %s: a null element survived as null, and "+
				"the table declares it coerced (%s). The destination gained null "+
				"elements and the published table now warns about nothing",
				key, columnType, rule.Rule)
		}
	default:
		return fmt.Sprintf("%s: the integration declares no list_element null "+
			"outcome, so what a null inside a list does here is untested and "+
			"unpublished", key)
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

// judgeTimestampInstant writes every temporal row with the host in a zone that
// is not UTC.
//
// Equality alone cannot make this claim. A timestamp written from a host in
// UTC-4 into a destination read from UTC-4 compares equal and is still four
// hours wrong for everyone else. That is what #153 was: clickhouse-go parsed a
// zone-less string in time.Local, so the stored value depended on where the
// process ran, and every test on a UTC machine agreed with the bug.
//
// Moving time.Local is process-wide, so this restores it before returning. It
// is safe because nothing under internal/ runs in parallel; a t.Parallel()
// anywhere in a package that reaches this code invalidates it.
func judgeTimestampInstant(t *testing.T, s TypeSubject) verdict {
	t.Helper()
	v := verdict{invariant: typeInstant}

	var temporal []coverage.TypeDecl
	for _, d := range s.Declared {
		if d.Outcome == "unsupported" {
			continue
		}
		if strings.HasPrefix(d.Key, "timestamp[") || d.Key == "date32" {
			temporal = append(temporal, d)
		}
	}
	if len(temporal) == 0 {
		v.failure = "the type table declares no temporal row, so nothing here " +
			"proves a timestamp keeps its instant"
		return v
	}

	// Deliberately not UTC, and deliberately not the zone the zoned value
	// carries. A runner that leaves the host in UTC proves nothing about a
	// leak, because the wrong answer and the right one coincide.
	restore := time.Local
	time.Local = time.FixedZone("conformance", 9*3600)
	defer func() { time.Local = restore }()

	for _, d := range temporal {
		if f := judgeTypeRow(t, s, d, false); f != "" {
			v.failure = "with the host nine hours ahead of UTC: " + f
			return v
		}
	}
	if f := judgeTemporalString(t, s); f != "" {
		v.failure = "with the host nine hours ahead of UTC: " + f
	}
	return v
}

// judgeTemporalString writes a timestamp that arrives as text into a temporal
// column.
//
// This is #153's exact shape, and no Arrow type describes it: the batch column
// is utf8 and the destination's is a DateTime, so a table keyed on Arrow types
// writes every timestamp as an Arrow timestamp and never as the string a
// handler passes through without a CAST. The driver parsed that string in
// time.Local, so the stored value moved with the host.
func judgeTemporalString(t *testing.T, s TypeSubject) string {
	t.Helper()

	ts := s.TemporalString
	if ts.Column == "" || ts.Value == "" || ts.Expect == "" {
		return "the integration declares no temporal_string case, so a timestamp " +
			"that arrives as text is unproven. That pairing is #153, and no Arrow " +
			"type describes it"
	}

	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	b.Append(ts.Value)

	arr := b.NewArray()
	defer arr.Release()

	dest := s.Prepare(t, "utf8", ts.Column)
	tbl := oneColumnTable(arr)
	defer tbl.Release()

	ctx := context.Background()
	err := dest.Sink.WriteTable(ctx, tbl)
	if err == nil {
		err = dest.Sink.Flush(ctx)
	}
	if err != nil {
		return fmt.Sprintf("%q into %s: the sink refused it: %v", ts.Value, ts.Column, err)
	}

	got, err := dest.ReadBack(t)
	if err != nil {
		return fmt.Sprintf("%q into %s: read back: %v", ts.Value, ts.Column, err)
	}
	if rendered, ok := got.(string); ok && rendered != ts.Expect {
		return fmt.Sprintf("%q into %s read back as %q, and the table expects %q. "+
			"A zone-less timestamp is UTC wherever the process runs",
			ts.Value, ts.Column, rendered, ts.Expect)
	}
	return ""
}

// fidelityCorpus is the set of strings a sink must carry unchanged.
//
// Each entry is here because something broke on it, or because something
// plausibly could. The escapes are #149, where jsonparser returned the bytes
// between the quotes undecoded and the sink stored them verbatim, so "a\nb"
// landed as four characters. The empty string is here because a destination
// that conflates it with NULL loses the difference between sending nothing
// and sending no characters. The rest cover the byte ranges and delimiters a
// naive length, escaping or encoding assumption mangles.
var fidelityCorpus = []struct {
	name  string
	value string
}{
	{"newline", "a\nb"},
	{"tab", "a\tb"},
	{"carriage return", "a\rb"},
	{"double quote", `say "hi"`},
	{"single quote", "it's"},
	{"backslash", `back\slash`},
	{"backtick and dollar", "`cmd` $var"},
	{"unicode", "L'Œil café 東京"},
	{"astral plane", "👁🌍"},
	{"combining marks", "éà"},
	{"empty", ""},
	{"leading and trailing space", "  padded  "},
	{"null-looking text", "NULL"},
	{"sql fragment", "'); DROP TABLE t; --"},
}

// judgeStringFidelity writes each corpus entry through the utf8 row and reads
// it back.
//
// One canonical value per type cannot catch this class of defect. #149
// corrupted every string carrying a backslash, and the sink's tests passed
// throughout because they all used tidy strings. The corpus is the test.
func judgeStringFidelity(t *testing.T, s TypeSubject) verdict {
	t.Helper()
	v := verdict{invariant: typeFidelity}

	var utf8Row *coverage.TypeDecl
	for i := range s.Declared {
		if s.Declared[i].Key == "utf8" {
			utf8Row = &s.Declared[i]
			break
		}
	}
	if utf8Row == nil {
		v.failure = "the type table has no utf8 row, so nothing here proves a " +
			"string survives the sink"
		return v
	}
	// A sink that cannot take a string at all has nothing to keep faithfully.
	if utf8Row.Outcome == "unsupported" {
		return v
	}

	for _, columnType := range utf8Row.Columns {
		for _, entry := range fidelityCorpus {
			if f := judgeOneString(t, s, columnType, entry.name, entry.value); f != "" {
				v.failure = f
				return v
			}
		}
	}
	return v
}

func judgeOneString(t *testing.T, s TypeSubject, columnType, name, value string) string {
	t.Helper()

	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	b.Append(value)

	arr := b.NewArray()
	defer arr.Release()

	dest := s.Prepare(t, "utf8", columnType)
	tbl := oneColumnTable(arr)
	defer tbl.Release()

	ctx := context.Background()
	err := dest.Sink.WriteTable(ctx, tbl)
	if err == nil {
		err = dest.Sink.Flush(ctx)
	}
	if err != nil {
		return fmt.Sprintf("the %s string into %s: the sink refused it: %v",
			name, columnType, err)
	}

	got, err := dest.ReadBack(t)
	if err != nil {
		return fmt.Sprintf("the %s string into %s: read back: %v", name, columnType, err)
	}
	if got == nil {
		return fmt.Sprintf("the %s string into %s read back as null. An empty "+
			"string is a value: conflating it with NULL loses the difference "+
			"between sending nothing and sending no characters", name, columnType)
	}
	if s, ok := got.(string); ok && s != value {
		return fmt.Sprintf("the %s string into %s read back as %q, and %q went in. "+
			"A string must survive byte for byte", name, columnType, s, value)
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
