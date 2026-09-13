package conformance

// The manager half of the harness.
//
// A table manager is a second loop that reaches a sink: it collects the
// windows that have closed, flushes them, and deletes them from the state
// table. The delete is its commit. So its claims are the consume loop's,
// restated for that commit, plus the two things a poll loop owes on its own:
// a closed window leaves, and a poll that cannot deliver stops the process
// rather than repeating forever.
//
// The second of those is #267. The manager logged a failed poll and polled
// again, so a window the destination rejected was collected, written and
// refused every tick for as long as the process lived, and the container
// reported healthy throughout.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"go.opentelemetry.io/otel/metric/noop"
)

// Manager is what a subject builds: the poll loop, and one poll of it.
type Manager interface {
	Start(ctx context.Context) error
	Poll(ctx context.Context) error
}

// ManagerSubject is one manager the harness can drive.
type ManagerSubject struct {
	// Integration is the registry id, e.g. "manager.tumbling_window".
	Integration string

	// New builds the manager around the sink the harness supplies, polling at
	// the given interval, with its final poll bounded by the budget. The
	// harness owns the sink so it can fail a flush and record what the
	// manager asked of it, and owns the budget so it can run one out.
	New func(t *testing.T, sink core.Sink, poll time.Duration, budget *core.DrainBudget) Manager

	// Seed replaces the state table's contents with n closed windows.
	Seed func(t *testing.T, n int)

	// Remaining returns how many closed windows the state table still holds.
	Remaining func(t *testing.T) int64
}

// Managers proves every manager invariant against the subject.
func Managers(t *testing.T, s ManagerSubject) {
	t.Helper()
	if s.Integration == "" {
		t.Fatal("conformance: ManagerSubject.Integration is required")
	}
	if s.New == nil || s.Seed == nil || s.Remaining == nil {
		t.Fatal("conformance: ManagerSubject needs New, Seed and Remaining")
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
	deleteAfterFlush    = "manager.delete.after_flush"
	deleteNothingOnFail = "manager.delete.nothing_on_failure"
	publishEventually   = "manager.publish.eventually"
	failureExits        = "manager.failure.exits"
	managerDrainBounded = "manager.drain.bounded"
)

// seededWindows is how many closed windows each check starts with. Two,
// so a manager that deletes one row per flush is caught.
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

	return []verdict{afterFlush, onFailure, eventually, exits, bounded}
}

// newManagerRun seeds the state table and builds the manager on a recording
// sink. The sink discards rows and records flushes, and fails every flush
// when asked, the way the pipeline harness's double-only subjects do.
func newManagerRun(t *testing.T, s ManagerSubject, fail bool) (Manager, *Recorder, *recordingSink) {
	t.Helper()
	s.Seed(t, seededWindows)
	rec := &Recorder{}
	sink := newRecordingSink(rec, nil, noop.NewMeterProvider())
	sink.fail = fail
	// The default deadline, so no check but the bounded one can run it out.
	budget := core.NewDrainBudget(core.DefaultDrainDeadline)
	t.Cleanup(budget.Stop)
	return s.New(t, sink.counted, managerPoll, budget), rec, sink
}

// checkManagerDrainBounded cancels Start against a sink that never answers
// and holds that it returns inside the drain deadline, with every closed
// window still in the state table for the next start to publish.
//
// The final poll is the manager's drain. Before #161 it ran on a context with
// no deadline, so a destination that stopped answering during shutdown held
// the process until the supervisor killed it.
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
	m := s.New(t, sink.counted, time.Hour, budget)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- m.Start(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err == nil {
			return fmt.Errorf("the sink never answered the final poll and Start " +
				"returned nil, so the shutdown reports windows published that were not")
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
		return fmt.Errorf("the final poll ran out of time and %d of %d closed windows "+
			"are gone from the state table. They were never delivered, so a restart "+
			"cannot publish them", seededWindows-left, seededWindows)
	}
	return nil
}

// checkDeleteAfterFlush holds that the state table still has every closed
// window at the moment the sink is flushed, and none once the poll returns.
//
// Observed from inside the flush rather than inferred from the count
// afterwards: a manager that deleted first and flushed second leaves the
// same empty table, and the difference is whether a failed flush loses the
// window.
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
			"the state table held %d of %d closed windows when the sink was "+
				"flushed. The delete ran first, so a flush that failed would "+
				"have lost them with no record anywhere",
			atFlush, seededWindows)
	}
	if left := s.Remaining(t); left != 0 {
		return fmt.Errorf(
			"the sink accepted %d windows and %d are still in the state "+
				"table, so the next poll publishes them again",
			seededWindows, left)
	}
	return nil
}

// checkDeleteNothingOnFailure fails the flush and holds every window still.
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
			"the sink refused the windows and %d of %d are gone from the "+
				"state table. A window the destination never took is lost, "+
				"and nothing downstream can tell",
			seededWindows-left, seededWindows)
	}
	return nil
}

// checkPublishEventually starts the loop over closed windows and holds that
// they reach the sink without anything else happening.
//
// The only liveness claim the delete checks leave open. A manager that never
// polled would satisfy both of them and fail this one alone.
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
			"%d closed windows sat in the state table for %s and the manager "+
				"never flushed them. A window that closes and is never "+
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
		return fmt.Errorf("the loop flushed and %d windows are still in the "+
			"state table", left)
	}
	return nil
}

// checkFailureExits starts the loop over a sink that rejects every flush and
// holds that Start returns, on its own, with the error.
//
// The sink runs its own retry ladder before an error reaches the manager,
// so what arrives is final: the destination refused the rows, or stayed
// unreachable past the deadline. A second attempt here is a retry the ladder
// already exhausted, and an unbounded series of them is #267: the same
// windows collected, written and refused every tick, with the process
// reporting healthy throughout.
func checkFailureExits(t *testing.T, s ManagerSubject) error {
	t.Helper()
	m, _, sink := newManagerRun(t, s, true)

	// Cancellable so a loop that never stops can still be torn down, but
	// never cancelled before the verdict: the claim is that it stops itself.
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
				"polling, %d attempts. Each poll collected the same windows and "+
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
			"the manager stopped and %d of %d windows are gone from the state "+
				"table. They were never delivered, so a restart cannot publish "+
				"them", seededWindows-left, seededWindows)
	}
	return nil
}
