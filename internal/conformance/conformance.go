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
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

	// ReadBack returns every row the destination holds. It must not go through
	// whatever Break breaks.
	ReadBack func(t *testing.T) []Row

	// OrderedReadBack says ReadBack returns rows in the order they arrived.
	//
	// False is the default because a destination that cannot report arrival
	// order is common and the failure is silent: an Iceberg scan returns rows
	// in data-file order, and a ClickHouse MergeTree returns them in its
	// ORDER BY key order, so both would satisfy preserves_order while
	// preserving nothing. The harness skips that claim rather than reading a
	// sorted list as evidence, and integrations.yml must carry the exemption.
	OrderedReadBack bool

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

	// Every subtest is its own entry in `go test -json`, so each one carries
	// both markers. The sink subtests happened to match a feature prefix --
	// TestIntegrationSinkClickhouse_Conformance/... reads as
	// TestSinkClickhouse* once the level prefix is stripped -- and that
	// accident is exactly what an explicit marker replaces.
	//
	// The feature comes from the registry rather than from the integration
	// id: sink.noop attributes to sink.noop, and a test_only integration
	// attributes to nothing.
	feature, hasFeature, err := coverage.FeatureFor(s.Integration)
	if err != nil {
		t.Fatalf("conformance: %v", err)
	}

	for _, v := range sinkVerdicts(t, s) {
		t.Run(v.invariant, func(t *testing.T) {
			// Emitted before the outcome, because it says which feature this
			// test touched rather than that it passed. A skipped or failing
			// subtest records as a skip or a failure against the feature,
			// which status() already refuses to count as coverage.
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

	if hasFeature {
		coverage.Covers(t, feature)
	}
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
	buffersOnly  = "sink.write.buffers_only"
	keepsBatch   = "sink.flush.keeps_batch"
	reportsDepth = "sink.buffer.reports_depth"
	emptyIsNoop  = "sink.flush.empty_is_noop"

	honoursContext  = "sink.flush.honours_context"
	noHollowSuccess = "sink.flush.no_hollow_success"
	preservesOrder  = "sink.flush.preserves_order"

	probeFailsStart = "sink.probe.fails_start"
	closeIdempotent = "lifecycle.close.idempotent"

	rowsCounted = "sink.rows.counted_on_delivery"
)

// deliveredRows reads sink_rows_written out of a manual reader.
//
// The counter is the only way to see what the pipeline will report to an
// operator. Asserting the sink's own view instead would prove nothing about
// the number a dashboard shows.
func deliveredRows(t *testing.T, r *sdkmetric.ManualReader, name string) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("conformance: collecting metrics: %v", err)
	}

	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

// prober mirrors sinks.Prober.
//
// Declared here rather than imported because internal/sinks imports this
// package in its tests, so importing it back would be a cycle in the test
// binary. Go interfaces are structural, so the two match without a dependency.
type prober interface {
	Probe(ctx context.Context) error
}

// probeTimeout is the deadline a Probe is given against a broken destination.
//
// probe() dials once and does not retry: nothing has been consumed yet, so
// there is nothing to lose by failing now, and the supervisor's restart is
// already the retry. A prober that ladders runs past twice this and is judged
// on that, not on whether it used the deadline it was handed.
const probeTimeout = 3 * time.Second

// sinkVerdicts runs one sequence and judges four invariants from it.
//
//  0. Flush with nothing buffered. The destination must stay empty --
//     empty_is_noop.
//  1. WriteTable(A) and Flush. The destination now holds A, and everything
//     below runs against a destination in the state production runs against.
//  2. WriteTable(B). The destination must not have gained B -- buffers_only --
//     and the sink must report one buffered row -- reports_depth.
//  3. Break, then Flush must fail. B must still be absent and the sink must
//     still report one row: what it could not deliver, it still owes.
//  4. Heal, then Flush must succeed. The sink must now report none.
//  5. The destination must hold A then B -- keeps_batch.
//
// The judgments are separate because a sink can hold any one without the
// others, and together they are what the pipeline depends on. A sink that
// delivers in WriteTable passes step 5 for the wrong reason: the row reached
// the destination before the fault, so nothing was ever kept, and the failed
// flush left the pipeline unable to commit offsets for rows that did go out.
// Step 2 is what catches it.
//
// A sink that discards its buffer on a failed flush passes steps 2 and 3 and
// holds nothing at step 5, which is the defect #221 fixed for ClickHouse.
//
// Step 1 is why the sequence starts with a delivery rather than a fault. A
// transport can behave differently before it has ever succeeded -- franz-go
// watches a Produce context only while a topic is unknown, so the Kafka sink
// took an abort path a resolved topic never reaches -- and a sink that only
// loses rows after its first successful flush is invisible to a sequence that
// never performs one.
func sinkVerdicts(t *testing.T, s SinkSubject) []verdict {
	t.Helper()

	if s.Break == nil {
		skip := "the subject has nothing to break; exempt " +
			s.Integration + " in integrations.yml"
		return []verdict{
			{invariant: emptyIsNoop, skipped: skip},
			{invariant: buffersOnly, skipped: skip},
			{invariant: keepsBatch, skipped: skip},
			{invariant: reportsDepth, skipped: skip},
			{invariant: honoursContext, skipped: skip},
			{invariant: noHollowSuccess, skipped: skip},
			{invariant: preservesOrder, skipped: skip},
			{invariant: probeFailsStart, skipped: skip},
			{invariant: closeIdempotent, skipped: skip},
			{invariant: rowsCounted, skipped: skip},
		}
	}

	// The subject's sink, wrapped exactly as sinks.New wraps it in production.
	// Judging the raw sink would leave sink_rows_written untested, which is
	// the whole reason that invariant is declared.
	//
	// A manual reader rather than the noop provider, because the assertion is
	// on the counter's value.
	reader := sdkmetric.NewManualReader()
	sink := core.NewCountingSink(
		s.New(t),
		sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
		s.Integration,
		"conformance",
	)
	ctx := context.Background()

	// Step 0. Nothing is buffered, so nothing may reach the destination. The
	// pipeline flushes on an interval whether or not a batch arrived, so a sink
	// that writes here writes on every idle tick.
	empty := verdict{invariant: emptyIsNoop}
	if err := sink.Flush(ctx); err != nil {
		empty.failure = "Flush with nothing buffered returned " + err.Error() +
			"; an idle flush interval must not fail the pipeline"
	} else if got := s.ReadBack(t); len(got) != 0 {
		empty.failure = "the destination holds " + describe(got) +
			" after a Flush when nothing was buffered"
	}

	// Step 1. One clean delivery, so every judgment below runs warm.
	warm := s.Table(t, 1)
	if err := sink.WriteTable(ctx, warm); err != nil {
		warm.Release()
		t.Fatalf("conformance: WriteTable during the pre-warm: %v", err)
	}
	if err := sink.Flush(ctx); err != nil {
		warm.Release()
		t.Fatalf("conformance: the pre-warm Flush failed against a healthy "+
			"destination: %v", err)
	}
	warm.Release()

	row := s.Table(t, 2)
	defer row.Release()

	if err := sink.WriteTable(ctx, row); err != nil {
		t.Fatalf("conformance: WriteTable before any fault: %v", err)
	}

	// Judged before anything can go wrong, so a write-through sink is named
	// for what it did rather than for a downstream symptom.
	//
	// The test is whether row 2 arrived, not how many rows are present. The
	// pre-warm already delivered row 1, and a sink that delivers at-least-once
	// may hold more than one copy of it.
	buffers := verdict{invariant: buffersOnly}
	if got := s.ReadBack(t); indexOf(got, Row{"id": int64(2)}) >= 0 {
		buffers.failure = "the destination holds " + describe(got) +
			" after WriteTable and before any Flush; only Flush may deliver, " +
			"because the pipeline commits offsets on what Flush reports"
	}

	// The sink's own account of what it is holding. The pipeline publishes it
	// as sink_buffered_rows, and a gauge nobody checks is a gauge that lies
	// quietly: an operator watching a buffer that stops draining needs this
	// number to be the truth.
	depth := verdict{invariant: reportsDepth}
	reporter, reports := sink.(core.BufferedRowReporter)
	if !reports {
		depth.skipped = s.Integration + " reports no buffer depth; exempt it " +
			"in integrations.yml or implement core.BufferedRowReporter"
	} else if n := reporter.BufferedRows(); n != 1 {
		depth.failure = "reports " + strconv.Itoa(n) +
			" buffered rows after one WriteTable; want 1"
	}

	s.Break(t)

	// Bounded well inside flushTimeout, so a sink that ignores its deadline is
	// caught by the gap between the two rather than by hanging the suite.
	deadline := flushTimeout / 4
	broken, cancel := context.WithTimeout(ctx, deadline)

	// Run off this goroutine, so a sink that ignores its context fails this
	// verdict rather than hanging the suite. Judging it after the call returned
	// would mean never judging it at all.
	type flushOutcome struct {
		err     error
		expired bool
	}
	done := make(chan flushOutcome, 1)
	go func() {
		e := sink.Flush(broken)
		// Read before cancel below, which reports Canceled whatever happened.
		done <- flushOutcome{err: e, expired: broken.Err() != nil}
	}()

	// Twice the deadline the sink was given. A sink that honours it returns
	// well inside this; one that ignores it does not return at all.
	grace := 2 * deadline

	honours := verdict{invariant: honoursContext}
	var (
		err     error
		expired bool
		hung    bool
	)
	select {
	case out := <-done:
		err, expired = out.err, out.expired
	case <-time.After(grace):
		hung = true
	}
	cancel()

	if hung {
		honours.failure = "Flush did not return within " + grace.String() +
			" against a context that expired after " + deadline.String() +
			"; the pipeline's drain reaches the sink only through that context, " +
			"so a sink that ignores it cannot be stopped"
		// The flush is still running, so nothing below can be judged against
		// this sink without racing it.
		stuck := "Flush never returned; honours_context names the defect"
		return []verdict{empty, buffers,
			{invariant: keepsBatch, skipped: stuck},
			depth, honours,
			{invariant: noHollowSuccess, skipped: stuck},
			{invariant: preservesOrder, skipped: stuck},
			{invariant: probeFailsStart, skipped: stuck},
			{invariant: closeIdempotent, skipped: stuck},
			{invariant: rowsCounted, skipped: stuck},
		}
	}

	switch {
	case err == nil:
		// Left to keeps_batch below, which explains why a flush into a broken
		// destination succeeding is a delivery defect rather than a timing one.
		honours.skipped = "Flush returned nil against a broken destination"
	case !expired:
		// The sink failed on its own before the deadline arrived, so no context
		// ended and there is nothing to honour. Only a fault that hangs
		// exercises this claim, which is a property of the subject's Break: a
		// full disk and a dropped table refuse immediately, a partition does
		// not.
		honours.skipped = s.Integration + " fails a flush before its context " +
			"expires, so no deadline was reached to honour; exempt it in " +
			"integrations.yml or give it a fault that hangs"
	case !errors.Is(err, context.DeadlineExceeded):
		honours.failure = "Flush returned " + err.Error() +
			", which does not wrap context.DeadlineExceeded; a caller cannot " +
			"tell a sink that gave up on time from one that failed outright"
	}

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
		return append([]verdict{empty, buffers, {
			invariant: keepsBatch,
			failure: "Flush returned nil while the destination was broken, " +
				"because the rows had already been delivered by WriteTable; " +
				"there was nothing left to keep",
		}, depth, honours,
			{invariant: noHollowSuccess, skipped: "the rows were delivered on write"},
			{invariant: preservesOrder, skipped: "the rows were delivered on write"},
			{invariant: rowsCounted, skipped: "the rows were delivered on write"},
		}, startAndStop(t, s)...)
	}

	if reports && depth.failure == "" {
		if n := reporter.BufferedRows(); n != 1 {
			depth.failure = "reports " + strconv.Itoa(n) +
				" buffered rows after a Flush that failed; want 1, because " +
				"the row is still owed"
		}
	}

	// keeps_batch is judged on what the retry delivers, not on what the
	// destination holds now.
	//
	// A failed Flush does not mean nothing arrived. It means nothing was
	// acknowledged. A produce request can reach the broker and be applied while
	// the response is lost, which is exactly what a partition looks like from
	// the client, and the sink cannot tell the two apart. Requiring the row to
	// be absent here asserts exactly-once against an at-least-once engine, and
	// a real broker behind a timeout toxic fails it while behaving correctly.
	//
	// The defect this used to aim at -- a sink that delivers before Flush --
	// is buffers_only's, judged above and before the fault.
	keeps := verdict{invariant: keepsBatch}

	// Step 4. A second row arrives while the destination is still broken, so
	// the retry has two rows to deliver and an order to deliver them in.
	third := s.Table(t, 3)
	if err := sink.WriteTable(ctx, third); err != nil {
		third.Release()
		t.Fatalf("conformance: WriteTable while the destination was broken: %v", err)
	}
	third.Release()

	if reports && depth.failure == "" {
		if n := reporter.BufferedRows(); n != 2 {
			depth.failure = "reports " + strconv.Itoa(n) +
				" buffered rows after a failed Flush and a second WriteTable; " +
				"want 2, because both rows are still owed"
		}
	}

	// Step 5. Flushing into the same fault must fail again. A sink that clears
	// its error state after reporting returns nil here having delivered
	// nothing, and the pipeline commits offsets for rows that never landed.
	hollow := verdict{invariant: noHollowSuccess}
	secondBroken, cancelSecond := context.WithTimeout(ctx, deadline)
	secondErr := sink.Flush(secondBroken)
	cancelSecond()

	// Two flushes have now failed. Only the pre-warm was ever acknowledged, so
	// the counter must still read 1: a counter that moved here would be
	// reporting delivery the destination never confirmed, which is #221 as a
	// metric.
	counted := verdict{invariant: rowsCounted}
	if n := deliveredRows(t, reader, "sink_rows_written"); n != 1 {
		counted.failure = "sink_rows_written is " + strconv.FormatInt(n, 10) +
			" after two failed flushes; want 1, from the pre-warm alone, " +
			"because a flush that failed delivered nothing"
	}
	if secondErr == nil && buffers.failure == "" {
		hollow.failure = "a second Flush into the same broken destination " +
			"returned nil; Flush may return nil only when every row since the " +
			"last success reached the destination"
	}

	s.Heal(t)
	if err := sink.Flush(ctx); err != nil {
		if keeps.failure == "" {
			keeps.failure = "Flush after Heal failed with " + err.Error() +
				"; the retry did not deliver what the failed flush kept"
		}
		return append([]verdict{empty, buffers, keeps, depth, honours, hollow,
			{invariant: preservesOrder, skipped: "the retry never delivered"},
			{invariant: rowsCounted, skipped: "the retry never delivered"}},
			startAndStop(t, s)...)
	}

	if reports && depth.failure == "" {
		if n := reporter.BufferedRows(); n != 0 {
			depth.failure = "reports " + strconv.Itoa(n) +
				" buffered rows after a Flush that succeeded; want 0"
		}
	}

	// The retry delivered rows 2 and 3, so the counter owes exactly those two
	// on top of the pre-warm. Counting them twice would be as wrong as not
	// counting them: the rows were delivered once.
	if counted.failure == "" {
		if n := deliveredRows(t, reader, "sink_rows_written"); n != 3 {
			counted.failure = "sink_rows_written is " + strconv.FormatInt(n, 10) +
				" after the retry delivered two kept rows; want 3, counting " +
				"each delivered row exactly once"
		}
	}

	got := s.ReadBack(t)
	want := []Row{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}

	if keeps.failure == "" {
		if bad := rowsArrived(got, want); bad != "" {
			keeps.failure = "after a failed Flush and a successful retry, " + bad
		}
	}

	// The same read-back, asked the other question. Loss belongs to
	// keeps_batch above; this is only about the order the rows arrived in.
	order := verdict{invariant: preservesOrder}
	switch {
	case !s.OrderedReadBack:
		order.skipped = s.Integration + " reads its destination back in an " +
			"order that is not the order rows arrived in, so this cannot be " +
			"judged; exempt it in integrations.yml"
	case keeps.failure != "":
		order.skipped = "rows were lost, so there is no order to judge"
	default:
		if bad := deliveredInOrder(got, want); bad != "" {
			order.failure = "after a failed Flush and a successful retry, " + bad
		}
	}

	return append([]verdict{empty, buffers, keeps, depth, honours, hollow, order, counted},
		startAndStop(t, s)...)
}

// startAndStop judges the two claims the delivery sequence cannot reach: that a
// Probe fails fast against a destination that is not there, and that Close
// survives a second call.
//
// It builds its own sink, because the one above holds a delivered buffer and
// both claims are about a sink that has not started yet.
func startAndStop(t *testing.T, s SinkSubject) []verdict {
	t.Helper()

	probes := verdict{invariant: probeFailsStart}
	closes := verdict{invariant: closeIdempotent}

	fresh := s.New(t)
	s.Break(t)
	defer s.Heal(t)

	if p, ok := fresh.(prober); !ok {
		probes.skipped = s.Integration + " implements no Prober, so there is " +
			"no start-time check to fail; exempt it in integrations.yml"
	} else {
		// The deadline is the probe's bound, and reaching it is a legitimate
		// way to fail: against a destination that hangs, nothing else can end
		// the wait. What the claim rules out is a probe that ladders past its
		// deadline, so the verdict measures against a grace well beyond it and
		// runs off this goroutine to catch one that never returns at all.
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		done := make(chan error, 1)
		go func() { done <- p.Probe(ctx) }()

		grace := 2 * probeTimeout
		select {
		case err := <-done:
			if err == nil {
				probes.failure = "Probe returned nil against a broken " +
					"destination; a probe that cannot fail certifies a " +
					"destination that is not there"
			}
		case <-time.After(grace):
			probes.failure = "Probe had not returned " + grace.String() +
				" after being given a " + probeTimeout.String() + " deadline; " +
				"it dials once and does not retry, because the supervisor's " +
				"restart is already the retry"
		}
		cancel()
	}

	if c, ok := fresh.(io.Closer); !ok {
		closes.skipped = s.Integration + " implements no Close, so there is " +
			"nothing to close twice; exempt it in integrations.yml"
	} else {
		_ = c.Close()
		if err := c.Close(); err != nil {
			closes.failure = "the second Close returned " + err.Error() +
				"; more than one shutdown path reaches Close, and the second " +
				"must not change the exit status"
		}
	}

	return []verdict{probes, closes}
}

// rowsArrived reports the first row of want that never reached the
// destination, if any.
//
// Loss is what keeps_batch claims: a row the sink could not deliver stays
// buffered and the next Flush re-attempts it. Whether the rows arrived in
// order is preserves_order's separate claim, and judging both here would blame
// keeps_batch for an ordering defect and send the reader to the wrong file.
func rowsArrived(got, want []Row) string {
	for _, w := range want {
		if indexOf(got, w) < 0 {
			return "id=" + format(w["id"]) + " never reached the destination; " +
				"want " + describe(want) + ", got " + describe(got)
		}
	}
	return ""
}

// deliveredInOrder judges a read-back against at-least-once delivery.
//
// sqlflow delivers at-least-once end to end: a row reaches the destination one
// or more times and is never lost. So this asks whether want appears in got as
// a subsequence. A repeat passes, because the pipeline commits offsets only
// after a flush returns nil and replays the batch when it does not, and the
// tumbling manager keeps a closed window in DuckDB until its flush succeeds.
// A missing row fails, and so does one that overtook a row written before it.
//
// It returns "" when the read-back honours the contract, and a sentence naming
// the failure otherwise.
func deliveredInOrder(got, want []Row) string {
	next := 0
	for _, w := range want {
		found := -1
		for j := next; j < len(got); j++ {
			if got[j]["id"] == w["id"] {
				found = j
				break
			}
		}
		if found >= 0 {
			next = found + 1
			continue
		}

		// Absent from the rest of the read-back. Either it never arrived, or it
		// arrived before a row written before it -- two different defects, and
		// the message has to send the reader to the right one.
		if indexOf(got, w) >= 0 {
			return "id=" + format(w["id"]) + " reached the destination out of " +
				"order, before a row written earlier; want " + describe(want) +
				" in that order, got " + describe(got)
		}
		return "id=" + format(w["id"]) + " never reached the destination; want " +
			describe(want) + ", got " + describe(got)
	}
	return ""
}

func indexOf(rows []Row, want Row) int {
	for i, r := range rows {
		if r["id"] == want["id"] {
			return i
		}
	}
	return -1
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
