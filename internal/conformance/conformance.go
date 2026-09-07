// Package conformance proves the invariants every integration must hold.
//
// It knows the invariants and nothing about any integration. An integration
// supplies a subject: how to build it, how to make its destination stop
// answering, how to bring it back, and how to read what it delivered. The
// harness runs one sequence and judges each invariant separately, so each
// gets its own subtest, its own marker, and its own cell in the matrix.
//
// Evidence is emitted, not inferred. A passing subtest logs
//
//	COVERS invariant=<id> integration=<id>
//
// which the coverage generator reads out of `go test -json` output. Test
// names carry nothing here, because the same code proves the same invariant
// for every integration and only the marker knows which one this run was.
//
// Invariants are declared in docs/coverage/invariants.yml. Adding one here
// without declaring it there reports as an unknown marker rather than as a
// cell.
package conformance

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
)

// Row is one row the destination holds, decoded by the subject through the
// destination's own reader. The harness compares rows by their "id" value
// only: what other columns a row carries is the destination's business.
type Row map[string]any

// SinkSubject is what an integration hands the harness.
type SinkSubject struct {
	// Integration is the integrations.yml id, e.g. "sink.clickhouse".
	Integration string

	// New builds the sink under test. Called once per sequence.
	New func(t *testing.T) core.Sink

	// Break makes the destination stop answering.
	//
	// Nil means the integration has nothing that can break, and the
	// network-shaped invariants are skipped. integrations.yml must then
	// exempt it with a reason, or the cell stays missing: the harness
	// skipping and the registry excusing are two statements that must agree.
	Break func(t *testing.T)

	// Heal reverses Break.
	Heal func(t *testing.T)

	// ReadBack returns every row the destination holds, in delivery order.
	// It must not go through whatever Break breaks.
	ReadBack func(t *testing.T) []Row

	// Table returns a one-row table with an int64 "id" column that the
	// destination accepts. The subject owns the schema because a ClickHouse
	// table has the columns its DDL declared, and the harness has no DDL.
	Table func(t *testing.T, id int64) arrow.Table
}

// flushTimeout bounds a Flush against a broken destination. A sink that hangs
// must fail this test rather than the suite.
const flushTimeout = 10 * time.Second

// Sinks proves every sink invariant the subject can exercise.
func Sinks(t *testing.T, s SinkSubject) {
	t.Helper()
	requireSubject(t, s)

	for _, v := range sinkVerdicts(t, s) {
		t.Run(v.invariant, func(t *testing.T) {
			if v.skipped != "" {
				t.Skip(v.skipped)
			}
			if v.failure != "" {
				t.Fatal(v.failure)
			}
			coverage.Invariant(t, v.invariant, s.Integration)
		})
	}

	// The feature axis credits the integration too, so a conformance run is
	// not invisible to features.yml.
	coverage.Covers(t, s.Integration)
}

func requireSubject(t *testing.T, s SinkSubject) {
	t.Helper()
	if s.Integration == "" {
		t.Fatal("conformance: SinkSubject.Integration is required")
	}
	if s.New == nil || s.ReadBack == nil || s.Table == nil {
		t.Fatalf("conformance: %s needs New, ReadBack and Table", s.Integration)
	}
	if (s.Break == nil) != (s.Heal == nil) {
		t.Fatalf("conformance: %s supplies one of Break and Heal; a fault that "+
			"cannot be reversed leaves the destination broken for the rest of "+
			"the run", s.Integration)
	}
}

// verdict is one invariant's outcome.
//
// Separated from the subtest that reports it so the harness's own tests can
// run it against a sink with a known defect and assert that it is caught,
// without themselves failing.
type verdict struct {
	invariant string
	failure   string // non-empty: the invariant does not hold
	skipped   string // non-empty: the subject cannot exercise it
}

const (
	buffersOnly = "sink.write.buffers_only"
	keepsBatch  = "sink.flush.keeps_batch"
)

// sinkVerdicts runs one sequence and judges two invariants from it.
//
//  1. WriteTable(A). The destination must still be empty -- buffers_only.
//  2. Break, then Flush must fail. The destination must still be empty:
//     rows that could not be delivered were kept, not sent.
//  3. Heal, then Flush must succeed.
//  4. The destination must hold exactly A, once -- keeps_batch.
//
// The two are separate because a sink can hold either without the other, and
// the pair is what the pipeline actually depends on. A sink that delivers in
// WriteTable passes step 4 for the wrong reason: the row reached the
// destination before the fault, so nothing was ever kept, and the failed
// flush left the pipeline unable to commit offsets for rows that did go out.
// That is the Kafka sink's shape, and step 1 is what catches it.
//
// A sink that discards its buffer on a failed flush passes steps 1 and 2 and
// holds nothing at step 4, which is the defect #221 fixed for ClickHouse.
func sinkVerdicts(t *testing.T, s SinkSubject) []verdict {
	t.Helper()

	if s.Break == nil {
		skip := "the subject has nothing to break; exempt " +
			s.Integration + " in integrations.yml"
		return []verdict{
			{invariant: buffersOnly, skipped: skip},
			{invariant: keepsBatch, skipped: skip},
		}
	}

	sink := s.New(t)
	row := s.Table(t, 1)
	defer row.Release()

	ctx := context.Background()
	if err := sink.WriteTable(ctx, row); err != nil {
		t.Fatalf("conformance: WriteTable before any fault: %v", err)
	}

	// Judged before anything can go wrong, so a write-through sink is named
	// for what it did rather than for a downstream symptom.
	buffers := verdict{invariant: buffersOnly}
	if got := s.ReadBack(t); len(got) != 0 {
		buffers.failure = "the destination holds " + describe(got) +
			" after WriteTable and before any Flush; only Flush may deliver, " +
			"because the pipeline commits offsets on what Flush reports"
	}

	s.Break(t)
	broken, cancel := context.WithTimeout(ctx, flushTimeout)
	err := sink.Flush(broken)
	cancel()

	if err == nil {
		// A flush that succeeds into a broken destination means one of two
		// things, and they point at different files.
		//
		// If the rows had already been delivered, this sink writes through:
		// there was nothing left for the flush to fail on. That is the sink's
		// bug, buffers_only already caught it, and reporting it as a broken
		// fault would send the reader to the subject instead.
		//
		// If nothing was delivered and the flush still succeeded, the fault
		// never took. That is the subject's bug and nothing can be judged.
		if buffers.failure == "" {
			t.Fatalf("conformance: Flush succeeded while the destination was "+
				"broken and nothing had been delivered, so %s's Break did not "+
				"break it", s.Integration)
		}
		return []verdict{buffers, {
			invariant: keepsBatch,
			failure: "Flush returned nil while the destination was broken, " +
				"because the rows had already been delivered by WriteTable; " +
				"there was nothing left to keep",
		}}
	}

	keeps := verdict{invariant: keepsBatch}
	if got := s.ReadBack(t); len(got) != 0 {
		keeps.failure = "the destination holds " + describe(got) +
			" after a Flush that failed; a row the sink could not deliver " +
			"must stay buffered rather than reach the destination unreported"
	}

	s.Heal(t)
	if err := sink.Flush(ctx); err != nil {
		if keeps.failure == "" {
			keeps.failure = "Flush after Heal failed with " + err.Error() +
				"; the retry did not deliver what the failed flush kept"
		}
		return []verdict{buffers, keeps}
	}

	got := s.ReadBack(t)
	if keeps.failure == "" && (len(got) != 1 || got[0]["id"] != int64(1)) {
		keeps.failure = "after a failed Flush and a successful retry the " +
			"destination holds " + describe(got) +
			"; want exactly the row the failed flush could not deliver"
	}
	return []verdict{buffers, keeps}
}

func describe(rows []Row) string {
	if len(rows) == 0 {
		return "no rows"
	}
	out := "rows ["
	for i, row := range rows {
		if i > 0 {
			out += ", "
		}
		out += "id=" + format(row["id"])
	}
	return out + "]"
}

func format(v any) string {
	if n, ok := v.(int64); ok {
		return strconv.FormatInt(n, 10)
	}
	return "?"
}
