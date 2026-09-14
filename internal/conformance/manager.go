package conformance

// The manager half of the harness.
//
// A watermark manager is a second loop that reaches a sink: it collects the
// buckets its watermark has passed, flushes them, deletes them, and advances
// the watermark. The delete and the watermark are its commit. So its claims
// are the consume loop's, restated for that commit, plus what a poll loop
// owes on its own: a closed bucket leaves, a poll that cannot deliver stops
// the process, and a drain ends. And three the watermark adds: it never
// moves backwards, it publishes committed rows only, and the late-row
// policy holds. And one the pipeline is owed: a window's I/O, however long,
// never fails a batch.
//
// The failure-exits claim is #267. The manager logged a failed poll and
// polled again, so a bucket the destination rejected was collected, written
// and refused every tick for as long as the process lived, and the container
// reported healthy throughout.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"go.opentelemetry.io/otel/metric/noop"
)

// Manager is what a subject builds: the poll loop, and one poll of it.
type Manager interface {
	Start(ctx context.Context) error
	Poll(ctx context.Context) error
}

// ManagerSubject is one manager the harness can drive.
type ManagerSubject struct {
	// Integration is the registry id, e.g. "manager.watermark".
	Integration string

	// New builds the manager around the sink the harness supplies, polling
	// at the given interval, with its final poll bounded by the budget and
	// the given late-row policy, "drop" or "reemit". The harness owns the
	// sink so it can fail a flush and record what the manager asked of it,
	// and owns the budget so it can run one out. Every call builds over the
	// same persisted state, which is how the watermark's memory is checked.
	New func(t *testing.T, sink core.Sink, poll time.Duration, budget *core.DrainBudget, late string) Manager

	// Seed replaces the table's contents with n closed buckets, the stream
	// idle for longer than the window's idle bound, so every one of them
	// closes on the next poll.
	Seed func(t *testing.T, n int)

	// SeedLate adds n rows to a bucket below the watermark, which a manager
	// has already closed. Called after a poll that closed what Seed wrote.
	SeedLate func(t *testing.T, n int)

	// Remaining returns how many rows the table still holds.
	Remaining func(t *testing.T) int64

	// Uncommitted writes n rows on the pipeline's connection inside a
	// transaction and leaves it open until release is called. Optional: a
	// subject that cannot hold a transaction open skips the committed-rows
	// claim.
	Uncommitted func(t *testing.T, n int) (release func())

	// Batch runs one batch of the consume loop on the pipeline's connection:
	// the handler ingests messages, runs its SQL, and re-initialises for the
	// next batch. Optional, with HoldCommit: a subject without both skips
	// the window-I/O claim.
	Batch func(t *testing.T) error

	// HoldCommit builds like New, polling only when asked, over a connection
	// whose commit calls hold first. By then the close has written its
	// delete and its watermark and committed neither.
	HoldCommit func(t *testing.T, sink core.Sink, budget *core.DrainBudget, late string, hold func()) Manager
}

// Managers proves every manager invariant against the subject.
func Managers(t *testing.T, s ManagerSubject) {
	t.Helper()
	if s.Integration == "" {
		t.Fatal("conformance: ManagerSubject.Integration is required")
	}
	if s.New == nil || s.Seed == nil || s.SeedLate == nil || s.Remaining == nil {
		t.Fatal("conformance: ManagerSubject needs New, Seed, SeedLate and Remaining")
	}

	feature, hasFeature, err := coverage.FeatureFor(s.Integration)
	if err != nil {
		t.Fatalf("conformance: %v", err)
	}

	for _, v := range managerVerdicts(t, s) {
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

	if hasFeature {
		coverage.Covers(t, feature)
	}
}

const (
	deleteAfterFlush       = "manager.delete.after_flush"
	deleteNothingOnFail    = "manager.delete.nothing_on_failure"
	publishEventually      = "manager.publish.eventually"
	failureExits           = "manager.failure.exits"
	managerDrainBounded    = "manager.drain.bounded"
	watermarkNeverRegress  = "manager.watermark.never_regresses"
	closeCommittedRowsOnly = "manager.close.committed_rows_only"
	latePolicyHolds        = "manager.late.policy_holds"
	batchIndependentOfIO   = "pipeline.batch.independent_of_window_io"
)

// seededWindows is how many closed buckets each check starts with. Two, so
// a manager that deletes one bucket per flush is caught.
const seededWindows = 2

// managerPoll is the interval the harness runs the loop at.
const managerPoll = 20 * time.Millisecond

// managerWait bounds every wait on the loop. It is many times the poll
// interval on purpose: a tighter bound asserts how fast the machine is,
// which is how #245 failed on CI.
const managerWait = 5 * time.Second

func managerVerdicts(t *testing.T, s ManagerSubject) []verdict {
	t.Helper()

	afterFlush := verdict{invariant: deleteAfterFlush}
	onFailure := verdict{invariant: deleteNothingOnFail}
	eventually := verdict{invariant: publishEventually}
	exits := verdict{invariant: failureExits}
	bounded := verdict{invariant: managerDrainBounded}
	regress := verdict{invariant: watermarkNeverRegress}
	committedOnly := verdict{invariant: closeCommittedRowsOnly}
	late := verdict{invariant: latePolicyHolds}
	independent := verdict{invariant: batchIndependentOfIO}

	if err := checkDeleteAfterFlush(t, s); err != nil {
		afterFlush.failure = err.Error()
	}
	if err := checkDeleteNothingOnFailure(t, s); err != nil {
		onFailure.failure = err.Error()
	}
	if err := checkPublishEventually(t, s); err != nil {
		eventually.failure = err.Error()
	}
	if err := checkFailureExits(t, s); err != nil {
		exits.failure = err.Error()
	}
	if err := checkManagerDrainBounded(t, s); err != nil {
		bounded.failure = err.Error()
	}
	if err := checkWatermarkNeverRegresses(t, s); err != nil {
		regress.failure = err.Error()
	}
	if s.Uncommitted == nil {
		committedOnly.skipped = s.Integration + " cannot hold rows uncommitted " +
			"on the pipeline's connection; supply Uncommitted"
	} else if err := checkCommittedRowsOnly(t, s); err != nil {
		committedOnly.failure = err.Error()
	}
	if err := checkLatePolicyHolds(t, s); err != nil {
		late.failure = err.Error()
	}
	if s.Batch == nil || s.HoldCommit == nil {
		independent.skipped = s.Integration + " cannot run a batch beside a " +
			"held close; supply Batch and HoldCommit"
	} else if err := checkBatchIndependentOfWindowIO(t, s); err != nil {
		independent.failure = err.Error()
	}

	return []verdict{afterFlush, onFailure, eventually, exits, bounded, regress,
		committedOnly, late, independent}
}

// newManagerRun seeds the table and builds the manager on a recording sink.
// The sink discards rows and records flushes, and fails every flush when
// asked, the way the pipeline harness's double-only subjects do.
func newManagerRun(t *testing.T, s ManagerSubject, fail bool) (Manager, *Recorder, *recordingSink) {
	t.Helper()
	s.Seed(t, seededWindows)
	rec := &Recorder{}
	sink := newRecordingSink(rec, nil, noop.NewMeterProvider())
	sink.fail = fail
	// The default deadline, so no check but the bounded one can run it out.
	budget := core.NewDrainBudget(core.DefaultDrainDeadline)
	t.Cleanup(budget.Stop)
	return s.New(t, sink.counted, managerPoll, budget, "reemit"), rec, sink
}

// checkDeleteAfterFlush holds that the table still has every closed bucket
// at the moment the sink is flushed, and none once the poll returns.
//
// Observed from inside the flush rather than inferred from the count
// afterwards: a manager that deleted first and flushed second leaves the
// same empty table, and the difference is whether a failed flush loses the
// bucket.
func checkDeleteAfterFlush(t *testing.T, s ManagerSubject) error {
	t.Helper()
	m, rec, _ := newManagerRun(t, s, false)

	atFlush := int64(-1)
	rec.onEvent = func(event string) {
		if event == "flush" {
			atFlush = s.Remaining(t)
		}
	}

	if err := m.Poll(context.Background()); err != nil {
		return fmt.Errorf("a poll with no fault injected failed: %v", err)
	}
	if !sameOrder(rec.Events(), []string{"flush"}) {
		return fmt.Errorf("the poll did %s; want one flush", list(rec.Events()))
	}
	if atFlush != seededWindows {
		return fmt.Errorf(
			"the table held %d of %d closed buckets when the sink was "+
				"flushed. The delete ran first, so a flush that failed would "+
				"have lost them with no record anywhere",
			atFlush, seededWindows)
	}
	if left := s.Remaining(t); left != 0 {
		return fmt.Errorf(
			"the sink accepted %d buckets and %d rows are still in the "+
				"table, so the next poll publishes them again",
			seededWindows, left)
	}
	return nil
}

// checkDeleteNothingOnFailure fails the flush and holds every bucket still.
func checkDeleteNothingOnFailure(t *testing.T, s ManagerSubject) error {
	t.Helper()
	m, rec, _ := newManagerRun(t, s, true)

	if err := m.Poll(context.Background()); err == nil {
		return fmt.Errorf("the flush failed and the poll did not")
	}
	if !sameOrder(rec.Events(), []string{"flush-failed"}) {
		return fmt.Errorf("after a failed flush the manager did %s; want "+
			"the failed flush and nothing else", list(rec.Events()))
	}
	if left := s.Remaining(t); left != seededWindows {
		return fmt.Errorf(
			"the sink refused the buckets and %d of %d are gone from the "+
				"table. A bucket the destination never took is lost, and "+
				"nothing downstream can tell",
			seededWindows-left, seededWindows)
	}
	// And the watermark did not move: a second poll publishes the same
	// buckets rather than treating them as late.
	rec2 := &Recorder{}
	sink2 := newRecordingSink(rec2, nil, noop.NewMeterProvider())
	budget := core.NewDrainBudget(core.DefaultDrainDeadline)
	defer budget.Stop()
	m2 := s.New(t, sink2.counted, managerPoll, budget, "drop")
	if err := m2.Poll(context.Background()); err != nil {
		return fmt.Errorf("the poll after a failed flush failed: %v", err)
	}
	if sink2.Rows() != seededWindows {
		return fmt.Errorf(
			"after a failed flush the next poll published %d of %d rows. The "+
				"watermark moved past buckets the sink never took, so under "+
				"drop they were discarded as late",
			sink2.Rows(), seededWindows)
	}
	return nil
}

// checkPublishEventually starts the loop over closed buckets and holds that
// they reach the sink without anything else happening.
func checkPublishEventually(t *testing.T, s ManagerSubject) error {
	t.Helper()
	m, rec, _ := newManagerRun(t, s, false)

	flushed := make(chan struct{}, 1)
	rec.onEvent = func(event string) {
		if event == "flush" {
			select {
			case flushed <- struct{}{}:
			default:
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()

	select {
	case <-flushed:
	case <-time.After(managerWait):
		cancel()
		<-done
		return fmt.Errorf(
			"%d closed buckets sat in the table for %s and the manager "+
				"never flushed them. A bucket that closes and is never "+
				"published is an aggregate nobody sees",
			seededWindows, managerWait)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("Start returned an error after a clean cancel: %v", err)
		}
	case <-time.After(managerWait):
		return fmt.Errorf("Start did not return within %s of its cancel", managerWait)
	}

	if left := s.Remaining(t); left != 0 {
		return fmt.Errorf("the loop flushed and %d rows are still in the table", left)
	}
	return nil
}

// checkFailureExits starts the loop over a sink that rejects every flush and
// holds that Start returns, on its own, with the error.
func checkFailureExits(t *testing.T, s ManagerSubject) error {
	t.Helper()
	m, _, sink := newManagerRun(t, s, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			return fmt.Errorf("the sink rejected every flush and Start returned nil")
		}
	case <-time.After(managerWait):
		cancel()
		<-done
		return fmt.Errorf(
			"the sink rejected every flush for %s and the manager kept "+
				"polling, %d attempts. Each poll collected the same buckets and "+
				"failed the same way, and the process stayed up. A supervisor "+
				"never notices, and the table never drains",
			managerWait, sink.Flushes())
	}

	if n := sink.Flushes(); n != 1 {
		return fmt.Errorf(
			"the manager attempted %d flushes before stopping. The sink ran "+
				"its retry ladder before the first one failed, so every attempt "+
				"after it repeats a retry the ladder already exhausted", n)
	}
	if left := s.Remaining(t); left != seededWindows {
		return fmt.Errorf(
			"the manager stopped and %d of %d buckets are gone from the "+
				"table. They were never delivered, so a restart cannot publish "+
				"them", seededWindows-left, seededWindows)
	}
	return nil
}

// checkManagerDrainBounded cancels Start against a sink that never answers
// and holds that it returns inside the drain deadline, with every closed
// bucket still in the table for the next start to publish.
func checkManagerDrainBounded(t *testing.T, s ManagerSubject) error {
	t.Helper()
	s.Seed(t, seededWindows)
	sink := newRecordingSink(&Recorder{}, nil, noop.NewMeterProvider())
	sink.hang = true
	sink.release = make(chan struct{})
	watchdog := time.AfterFunc(drainBoundedWait, func() { close(sink.release) })
	defer watchdog.Stop()
	budget := core.NewDrainBudget(drainBudget)
	defer budget.Stop()
	// An hour between polls, so the only poll that runs is the final one.
	m := s.New(t, sink.counted, time.Hour, budget, "reemit")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- m.Start(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err == nil {
			return fmt.Errorf("the sink never answered the final poll and Start " +
				"returned nil, so the shutdown reports buckets published that were not")
		}
	case <-time.After(2 * drainBoundedWait):
		return fmt.Errorf("a %s drain deadline held the final poll past %s against "+
			"a sink that never answered, and the watchdog could not release it",
			drainBudget, 2*drainBoundedWait)
	}
	if took := time.Since(started); took >= drainBoundedWait {
		return fmt.Errorf("a %s drain deadline held the final poll for %s against "+
			"a sink that never answered", drainBudget, took)
	}
	if sink.Flushes() != 1 {
		return fmt.Errorf("the final poll flushed %d times; want the one attempt the "+
			"deadline ended", sink.Flushes())
	}
	if left := s.Remaining(t); left != seededWindows {
		return fmt.Errorf("the final poll ran out of time and %d of %d closed buckets "+
			"are gone from the table. They were never delivered, so a restart "+
			"cannot publish them", seededWindows-left, seededWindows)
	}
	return nil
}

// checkWatermarkNeverRegresses closes buckets, then builds a second manager
// over the same persisted state and holds that it publishes nothing the
// first one published: the watermark it loads is the one the first one
// saved, and a table that now holds older rows does not pull it back.
func checkWatermarkNeverRegresses(t *testing.T, s ManagerSubject) error {
	t.Helper()
	m, _, sink := newManagerRun(t, s, false)
	if err := m.Poll(context.Background()); err != nil {
		return fmt.Errorf("the first close failed: %v", err)
	}
	if sink.Rows() != seededWindows {
		return fmt.Errorf("the first close published %d of %d rows", sink.Rows(), seededWindows)
	}

	// Rows for a bucket the first manager closed, seen by a manager that
	// has never run. Its only memory is what the first one persisted.
	s.SeedLate(t, 1)
	rec2 := &Recorder{}
	sink2 := newRecordingSink(rec2, nil, noop.NewMeterProvider())
	budget := core.NewDrainBudget(core.DefaultDrainDeadline)
	defer budget.Stop()
	m2 := s.New(t, sink2.counted, managerPoll, budget, "drop")
	if err := m2.Poll(context.Background()); err != nil {
		return fmt.Errorf("the second manager's poll failed: %v", err)
	}
	if sink2.Rows() != 0 {
		return fmt.Errorf(
			"a second manager over the same state published %d rows of a "+
				"bucket the first one closed. It started from a lower "+
				"watermark than the one saved, so a restart publishes buckets "+
				"twice", sink2.Rows())
	}
	if left := s.Remaining(t); left != 0 {
		return fmt.Errorf("under drop, %d late rows are still in the table", left)
	}
	return nil
}

// checkCommittedRowsOnly holds rows open in the pipeline's transaction and
// holds that a close does not publish them. A manager that shared the
// pipeline's connection saw them, and a batch that then rolled back had
// been counted in a bucket the sink already held.
func checkCommittedRowsOnly(t *testing.T, s ManagerSubject) error {
	t.Helper()
	m, _, sink := newManagerRun(t, s, false)
	release := s.Uncommitted(t, seededWindows)
	defer release()

	if err := m.Poll(context.Background()); err != nil {
		return fmt.Errorf("a poll beside an open transaction failed: %v", err)
	}
	if sink.Rows() != seededWindows {
		return fmt.Errorf(
			"the sink received %d rows for %d committed. The close read the "+
				"pipeline's uncommitted rows, so a batch that rolls back has "+
				"already been published", sink.Rows(), seededWindows)
	}
	return nil
}

// checkLatePolicyHolds closes buckets, seeds rows for one of them, and holds
// that drop discards them without a flush and reemit publishes them once.
func checkLatePolicyHolds(t *testing.T, s ManagerSubject) error {
	t.Helper()
	for _, policy := range []string{"drop", "reemit"} {
		s.Seed(t, seededWindows)
		rec := &Recorder{}
		sink := newRecordingSink(rec, nil, noop.NewMeterProvider())
		budget := core.NewDrainBudget(core.DefaultDrainDeadline)
		m := s.New(t, sink.counted, managerPoll, budget, policy)
		if err := m.Poll(context.Background()); err != nil {
			budget.Stop()
			return fmt.Errorf("%s: the first close failed: %v", policy, err)
		}

		s.SeedLate(t, 3)
		if err := m.Poll(context.Background()); err != nil {
			budget.Stop()
			return fmt.Errorf("%s: the poll after the late rows failed: %v", policy, err)
		}
		budget.Stop()

		flushes, rows, left := sink.Flushes(), sink.Rows(), s.Remaining(t)
		switch policy {
		case "drop":
			if flushes != 1 || rows != seededWindows {
				return fmt.Errorf(
					"drop: late rows for a closed bucket were published: %d flushes "+
						"and %d rows, want 1 and %d. An append-only sink now holds "+
						"the bucket twice", flushes, rows, seededWindows)
			}
			if left != 0 {
				return fmt.Errorf("drop: %d late rows are still in the table", left)
			}
		case "reemit":
			if flushes != 2 || rows != seededWindows+3 {
				return fmt.Errorf(
					"reemit: late rows for a closed bucket were not published: %d "+
						"flushes and %d rows, want 2 and %d", flushes, rows, seededWindows+3)
			}
			if left != 0 {
				return fmt.Errorf("reemit: %d late rows are still in the table after "+
					"being published", left)
			}
		}
	}
	return nil
}

// batchesDuringHold is how many batches run while a close is held. More
// than one, so a handler that fails only its second batch after a refused
// statement is caught.
const batchesDuringHold = 3

// checkBatchIndependentOfWindowIO holds a window's I/O open and runs batches
// beside it, twice: while the sink's flush has not returned, and while a
// close has written its delete and its watermark and not committed them.
// Every batch must succeed, and the held close must still land.
//
// The second hold is the one that failed in production (the bluesky demo on
// v2026.09.14). The structured handler checkpoints as it re-initialises,
// DuckDB refuses a checkpoint while another connection holds an uncommitted
// UPDATE, and saving the watermark is one. The refusal failed the batch,
// and the batch's failure stopped the process. #283's rule, that every
// statement on the pipeline's connection holds its lock, stopped reaching
// the window when #281 moved it to connections of its own, and no invariant
// said so.
func checkBatchIndependentOfWindowIO(t *testing.T, s ManagerSubject) error {
	t.Helper()
	budget := core.NewDrainBudget(core.DefaultDrainDeadline)
	defer budget.Stop()

	var flushHeld, commitHeld atomic.Bool
	var entered, release chan struct{}
	hold := func(armed *atomic.Bool) {
		if armed.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
	}

	s.Seed(t, seededWindows)
	rec := &Recorder{}
	rec.onEvent = func(event string) {
		if event == "flush" {
			hold(&flushHeld)
		}
	}
	sink := newRecordingSink(rec, nil, noop.NewMeterProvider())
	m := s.HoldCommit(t, sink.counted, budget, "reemit", func() { hold(&commitHeld) })

	during := func(armed *atomic.Bool, what string) error {
		entered, release = make(chan struct{}), make(chan struct{})
		armed.Store(true)
		polled := make(chan error, 1)
		go func() { polled <- m.Poll(context.Background()) }()

		select {
		case <-entered:
		case err := <-polled:
			return fmt.Errorf("the close returned before reaching %s: %v", what, err)
		case <-time.After(managerWait):
			close(release)
			return fmt.Errorf("the close did not reach %s within %s", what, managerWait)
		}

		var batchErr error
		for i := 0; i < batchesDuringHold && batchErr == nil; i++ {
			if err := s.Batch(t); err != nil {
				batchErr = fmt.Errorf(
					"batch %d of %d, run while the window held %s, failed: %v. "+
						"The consume loop stops on a failed batch, so a window "+
						"whose I/O outlasts a batch stops the pipeline",
					i+1, batchesDuringHold, what, err)
			}
		}
		close(release)

		select {
		case err := <-polled:
			if batchErr != nil {
				return batchErr
			}
			if err != nil {
				return fmt.Errorf("the close held at %s failed once released: %v", what, err)
			}
		case <-time.After(managerWait):
			return fmt.Errorf("the close held at %s did not finish within %s of its release", what, managerWait)
		}
		return nil
	}

	// A sink write that has not returned. The first close also saves the
	// window's first watermark, so the next one updates it.
	if err := during(&flushHeld, "its sink's flush"); err != nil {
		return err
	}
	// A late row under reemit makes the next close publish, delete and
	// update the watermark's row, then stop before committing.
	s.SeedLate(t, 1)
	if err := during(&commitHeld, "its uncommitted delete and watermark"); err != nil {
		return err
	}
	if left := s.Remaining(t); left != 0 {
		return fmt.Errorf("both held closes finished and %d rows are still in the table", left)
	}
	return nil
}

var _ = errs.CodeOf
