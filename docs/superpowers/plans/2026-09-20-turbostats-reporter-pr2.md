# TurboStats Reporter, Engine PR 2: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An instance delivers itself. `sqlflow run` and `sqlflow serve` post signed TurboStats bundles outbound on an interval, and a final one on a clean shutdown.

**Architecture:** One `internal/turbostats/reporter.go` that owns a goroutine, a ticker and an HTTP client. It calls the same `Collect` the HTTP route calls and signs with the public `turbostats/wire` package. The config block is identical under `pipeline` and `serve`. Nothing it does can block the consume loop or delay a shutdown past its own timeout.

**Tech Stack:** Go 1.26, `net/http`, `turbostats/wire`, `internal/config`, `zeebo/assert`, pytest for the release suite.

**Spec:** `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md`, the **The reporter** and **Signing** sections, plus the reporter rules in `2026-09-10-turbostats-v1-design.md` that it says still hold. Read both.

**The receiver already exists.** `turbolytics/sql-flow-control` accepts these bundles today: `POST /v1/turbostats`, Ed25519 over five lines, `{"v":1,"commands":[]}` in reply. Its `internal/ingest` is the thing this plan must satisfy, and Task 5 drives the two together.

## Global Constraints

- **A failed post never touches the pipeline.** The reporter runs on its own goroutine, every post is bounded by its timeout, and nothing it does can block the consume loop or delay a shutdown by more than that timeout.
- **A failed post is logged and forgotten.** One warning when a run of failures begins, one info when it ends, debug per failure. No retry within an interval, no queue. A missed interval is a gap, and the gap is the signal the control plane reads.
- **Any 2xx is success**, whatever the body holds. Parse the response as an object, ignore unknown fields, ignore `commands` entirely. A body that does not parse is logged at debug and is still a success. This rule is why instances deployed today survive the day commands ship.
- **Plaintext to a non-loopback address fails config load.** TLS is not optional on a public network, and a device is on one.
- The interval carries ten percent jitter, so a fleet that loses power together does not report together.
- `--turbostats` and `report_to` are independent. A fleet instance usually sets only the second, and the route stays off.
- Signing, the canonical string, the key id and the header names come from `turbostats/wire`. Do not reimplement any of them.
- All prose follows `CLAUDE.md`: active voice, short sentences, comments explain why.
- Commit messages name the defect or the need, the change, and the evidence, and state what breaks if the change is wrong. No attribution lines, no session links.
- Work on a branch. Never commit to `main`.

## File Structure

| File | Responsibility |
|---|---|
| `internal/config/turbostats.go` (create) | The `turbostats` block and its validation |
| `internal/config/config.go`, `serve.go` (modify) | Hang the block off `pipeline` and `serve` |
| `internal/turbostats/reporter.go` (create) | The goroutine, the ticker, the post |
| `internal/cli/run/root.go`, `metrics.go` (modify) | Start it, and send the final bundle on drain |
| `internal/cli/serve/serve.go` (modify) | The same, after the HTTP server drains |
| `docs/coverage/features.yml` (modify) | `observability.turbostats.reporter` |
| `tests/release/test_image.py` (modify) | The shipped image reports to a receiver |

---

### Task 1: The config block

**Files:**
- Create: `internal/config/turbostats.go`
- Modify: `internal/config/config.go` (add to `Pipeline`), `internal/config/serve.go` (add to `Serve`)
- Test: `internal/config/turbostats_test.go`

**Interfaces:**
- `type TurboStats struct { ID, ReportTo, Key string; IntervalSeconds int; Allow []string }`
- `func (t *TurboStats) Check() []Violation` following this package's existing validation shape
- `func (t *TurboStats) Interval() time.Duration`, defaulting to 60 seconds
- `func (t *TurboStats) Enabled() bool`, which is `ReportTo != ""`

Read `internal/config/serve.go`'s `Check` methods first and follow their shape exactly. A new validation style in one block is a validation style nobody maintains.

- [ ] **Step 1: Create the branch**

```bash
git switch main && git pull && git switch -c turbostats/reporter-pr2
```

- [ ] **Step 2: Write the failing test**

Create `internal/config/turbostats_test.go`:

```go
package config

import (
	"testing"

	"github.com/zeebo/assert"
)

func valid() *TurboStats {
	return &TurboStats{
		ID:       "bluesky-01",
		ReportTo: "https://control.turbolytics.io/v1/turbostats",
		Key:      "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
	}
}

func TestTurboStats_AcceptsAWholeBlock(t *testing.T) {
	assert.Equal(t, 0, len(valid().Check()))
	assert.That(t, valid().Enabled())
	// Sixty seconds unless asked otherwise: a fleet page derives rates from
	// consecutive bundles, and a minute is dense enough to see a rate change.
	assert.Equal(t, 60, int(valid().Interval().Seconds()))
}

// Reporting is off until report_to names somewhere. An instance with no
// control plane is the ordinary case, not a misconfiguration.
func TestTurboStats_IsOffWithoutAReportTo(t *testing.T) {
	var off TurboStats
	assert.That(t, !off.Enabled())
	assert.Equal(t, 0, len(off.Check()))
}

// The control plane files a bundle under instance.id. Without one it cannot
// name what reported, so a config that enables reporting without it fails
// before the pipeline starts rather than on the first post.
func TestTurboStats_RequiresAnIDWhenReporting(t *testing.T) {
	c := valid()
	c.ID = ""
	assert.Equal(t, 1, len(c.Check()))
}

// TLS is not optional on a public network, and a device is on one. A
// plaintext report_to would put a signed bundle and its headers on the wire
// in the clear.
func TestTurboStats_RefusesPlaintextToTheInternet(t *testing.T) {
	c := valid()
	c.ReportTo = "http://control.turbolytics.io/v1/turbostats"
	assert.Equal(t, 1, len(c.Check()))

	// Loopback is how someone tests against a control plane on their own
	// machine, and there is no network to eavesdrop.
	for _, url := range []string{
		"http://127.0.0.1:8090/v1/turbostats",
		"http://localhost:8090/v1/turbostats",
		"http://[::1]:8090/v1/turbostats",
	} {
		c.ReportTo = url
		assert.Equal(t, 0, len(c.Check()))
	}
}

// A key that cannot sign is a fleet that never reports, and the operator
// finds out from a silent page rather than a failed start.
func TestTurboStats_RefusesAKeyThatCannotSign(t *testing.T) {
	for _, key := range []string{"", "not-a-credential", "sfc_short"} {
		c := valid()
		c.Key = key
		assert.That(t, len(c.Check()) > 0)
	}
}

// v1 issues one scope. A config naming another describes a permission this
// build does not implement, and accepting it silently would be worse than
// refusing it.
func TestTurboStats_AcceptsOnlyTheReadScope(t *testing.T) {
	c := valid()
	c.Allow = []string{"read"}
	assert.Equal(t, 0, len(c.Check()))

	c.Allow = []string{"read", "execute"}
	assert.Equal(t, 1, len(c.Check()))
}

// An interval under a second is a mistake that would hammer a control plane,
// and a negative one is not a duration.
func TestTurboStats_RefusesAnImpossibleInterval(t *testing.T) {
	for _, s := range []int{-1, 0} {
		c := valid()
		c.IntervalSeconds = s
		// Zero is absent, which is the default.
		if s == 0 {
			assert.Equal(t, 0, len(c.Check()))
			continue
		}
		assert.Equal(t, 1, len(c.Check()))
	}
}
```

- [ ] **Step 3: Run it, confirm it fails, then implement**

Run: `go test ./internal/config/ -run TestTurboStats`
Expected: FAIL, `undefined: TurboStats`.

The implementation validates in this order, because the first failure is the one an operator reads: `report_to` parses and is HTTPS or loopback, `id` is present, `key` parses with `wire.ParseCredential`, every `allow` entry is `read`, and `interval_seconds` is absent or positive.

`Check` returns this package's existing violation type. **Do not invent a new one.**

- [ ] **Step 4: Hang the block off both configs**

`Pipeline` gains `TurboStats *TurboStats \`yaml:"turbostats,omitempty"\`` and `Serve` gains the same. Both `Check` methods call into it when the pointer is set.

- [ ] **Step 5: Run the config suite**

Run: `go test -short ./internal/config/...`
Expected: PASS, including every existing test. A new field must not change how any current config parses.

- [ ] **Step 6: Prove `validate` reports it**

The engine's own rule is that a config error is found before a pipeline starts. Write a fixture with a bad key and run the real command:

```bash
go build -o bin/sqlflow ./cmd/sqlflow
cat > /tmp/bad.yml <<'YML'
pipeline:
  name: demo
  turbostats:
    id: demo-01
    report_to: https://control.turbolytics.io/v1/turbostats
    key: not-a-credential
YML
./bin/sqlflow validate /tmp/bad.yml; echo "exit: $?"
```

Expected: a message naming `key`, and a non-zero exit. Paste it into the commit.

- [ ] **Step 7: Commit**

The message names what a bad key costs: a fleet that never reports, discovered from a silent page rather than a failed start.

---

### Task 2: The reporter

**Files:**
- Create: `internal/turbostats/reporter.go`
- Test: `internal/turbostats/reporter_test.go`

**Interfaces:**
- `type Reporter struct{ ... }`
- `func NewReporter(conf ReporterConfig) (*Reporter, error)`
- `type ReporterConfig struct { ReportTo string; Key ed25519.PrivateKey; Interval time.Duration; Collect func(context.Context) (Bundle, error); Log *zap.Logger; Now func() time.Time; Client *http.Client }`
- `func (r *Reporter) Run(ctx context.Context)` — blocks until ctx ends
- `func (r *Reporter) Final(ctx context.Context, exit Exit)` — one last post, bounded

**The rules this task implements, from the spec:**

1. One post at start, one per interval with ten percent jitter, one on a clean drain carrying `exit`.
2. A ten-second timeout per post. Nothing waits longer, including `Final`.
3. Any 2xx is success. The body is read and ignored; an unparseable one is debug, not an error.
4. A failure logs once at the start of a run and once at its end. No retry, no queue.

- [ ] **Step 1: Write the failing test**

Create `internal/turbostats/reporter_test.go`:

```go
package turbostats

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

// receiver records what arrived and answers however a test asks it to.
type receiver struct {
	mu      sync.Mutex
	bodies  [][]byte
	headers []http.Header
	status  int
	reply   string
	hang    chan struct{}
}

func newReceiver() *receiver {
	return &receiver{status: http.StatusOK, reply: `{"v":1,"commands":[]}`}
}

func (rc *receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	rc.mu.Lock()
	rc.bodies = append(rc.bodies, body)
	rc.headers = append(rc.headers, r.Header.Clone())
	hang, status, reply := rc.hang, rc.status, rc.reply
	rc.mu.Unlock()
	if hang != nil {
		<-hang
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(reply))
}

func (rc *receiver) count() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return len(rc.bodies)
}

func testKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	priv, err := wire.ParseCredential("sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
	assert.NoError(t, err)
	return priv
}

func testReporter(t *testing.T, url string, key ed25519.PrivateKey) *Reporter {
	t.Helper()
	r, err := NewReporter(ReporterConfig{
		ReportTo: url, Key: key, Interval: 10 * time.Millisecond,
		Collect: func(context.Context) (Bundle, error) {
			return Bundle{V: Version, Instance: Instance{ID: "one"}}, nil
		},
		Log: zap.NewNop(),
	})
	assert.NoError(t, err)
	return r
}

// The first bundle goes at start, not one interval later. An instance that
// waited would be missing from a fleet page for its first minute, which is
// exactly when someone is watching a deploy.
func TestReporter_PostsAtStartAndOnTheInterval(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go testReporter(t, srv.URL, testKey(t)).Run(ctx)

	waitFor(t, func() bool { return rc.count() >= 3 })
}

// The receiver must be able to verify what arrived, with the key id it can
// look the public half up by.
func TestReporter_SignsEveryPost(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()
	key := testKey(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go testReporter(t, srv.URL, key).Run(ctx)
	waitFor(t, func() bool { return rc.count() >= 1 })

	rc.mu.Lock()
	body, header := rc.bodies[0], rc.headers[0]
	rc.mu.Unlock()

	assert.Equal(t, MediaType, header.Get("Content-Type"))
	keyID, ts, sig, err := wire.ParseHeaders(header)
	assert.NoError(t, err)
	assert.Equal(t, wire.KeyID(key.Public().(ed25519.PublicKey)), keyID)
	// The path is the receiver's, and the query is not signed.
	assert.That(t, wire.Verify(key.Public().(ed25519.PublicKey),
		http.MethodPost, "/", ts, body, sig))
}

// The rule that keeps deployed instances alive the day commands ship. A
// reporter that failed on an unknown field would strand every one of them.
func TestReporter_TreatsAny2xxAsSuccess(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	for _, reply := range []string{
		`{"v":1,"commands":[]}`,
		`{"v":1,"commands":[{"id":"c1","verb":"restart"}],"later":true}`,
		`{"v":2,"unknown":{"deeply":"nested"}}`,
		`not json at all`,
		``,
	} {
		rc := newReceiver()
		rc.reply = reply
		rc.status = http.StatusAccepted
		srv := httptest.NewServer(rc)

		ctx, cancel := context.WithCancel(context.Background())
		go testReporter(t, srv.URL, testKey(t)).Run(ctx)
		waitFor(t, func() bool { return rc.count() >= 2 })
		cancel()
		srv.Close()
	}
}

// A receiver that is down, refusing, or hanging must not stop the reporter
// posting. The control plane reads the gap; the instance keeps working.
func TestReporter_KeepsGoingThroughFailures(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	rc.status = http.StatusUnauthorized
	srv := httptest.NewServer(rc)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go testReporter(t, srv.URL, testKey(t)).Run(ctx)

	// An unregistered fleet posts forever and never gives up: the operator
	// registers the key later and the instance appears with no restart.
	waitFor(t, func() bool { return rc.count() >= 3 })
}

// A hung receiver must not hold a shutdown. This is the rule that keeps the
// reporter from being the reason a pod takes thirty seconds to die.
func TestReporter_AHungReceiverDoesNotDelayTheCaller(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	rc.hang = make(chan struct{})
	srv := httptest.NewServer(rc)
	defer func() { close(rc.hang); srv.Close() }()

	r, err := NewReporter(ReporterConfig{
		ReportTo: srv.URL, Key: testKey(t), Interval: time.Hour,
		Collect: func(context.Context) (Bundle, error) { return Bundle{V: Version}, nil },
		Log:     zap.NewNop(),
		Client:  &http.Client{Timeout: 50 * time.Millisecond},
	})
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		r.Final(context.Background(), Exit{Reason: "SIGTERM"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Final waited on a hung receiver")
	}
}

// The last bundle is how a control plane tells a clean stop from a crash. A
// process that died cannot send one, which is the whole signal.
func TestReporter_TheFinalBundleCarriesTheExit(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()

	testReporter(t, srv.URL, testKey(t)).Final(context.Background(),
		Exit{Reason: "SIGTERM", Code: 0})
	assert.Equal(t, 1, rc.count())

	rc.mu.Lock()
	body := rc.bodies[0]
	rc.mu.Unlock()
	var b Bundle
	assert.NoError(t, json.Unmarshal(body, &b))
	assert.That(t, b.Exit != nil)
	assert.Equal(t, "SIGTERM", b.Exit.Reason)
}

// A collect that fails is not a post. Sending a blank bundle would put a
// healthy-looking document on a page that should show a gap.
func TestReporter_ACollectFailureSendsNothing(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()

	r, err := NewReporter(ReporterConfig{
		ReportTo: srv.URL, Key: testKey(t), Interval: 10 * time.Millisecond,
		Collect: func(context.Context) (Bundle, error) {
			return Bundle{}, errors.New("reader closed")
		},
		Log: zap.NewNop(),
	})
	assert.NoError(t, err)
	r.Final(context.Background(), Exit{Reason: "SIGTERM"})
	assert.Equal(t, 0, rc.count())
}

// waitFor polls until cond holds or the test gives up. A sleep long enough to
// be reliable is a sleep that slows every run.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition never held")
}
```

Add `"errors"` to the imports.

- [ ] **Step 2: Run it, confirm it fails, then implement**

Points the implementation must get right, each of which a test above pins:

- **The interval jitters by ten percent**, computed per tick, so a fleet restarted together drifts apart rather than staying in lockstep.
- **`Run` posts before its first tick.**
- **Every post uses a context with the timeout**, derived from the caller's, so a cancelled run does not wait for a hanging receiver.
- **The response body is drained and closed** whatever the status, or the connection is not reused and a busy instance leaks sockets.
- **A failure run is logged at its edges.** Keep a `failing bool`; log `Warn` on the transition into it and `Info` on the transition out, with `Debug` per failure. This is the same pattern the control plane's rate limiter uses, and for the same reason.

- [ ] **Step 3: Run with the race detector**

Run: `go test -race -count=3 ./internal/turbostats/`
Expected: PASS. The reporter shares a bundle builder with an HTTP handler, so this is the test that matters.

- [ ] **Step 4: Mutate three rules**

| Mutation | Test that must fail |
|---|---|
| `Run` waits for its first tick before posting | `TestReporter_PostsAtStartAndOnTheInterval` |
| A non-2xx stops the loop | `TestReporter_KeepsGoingThroughFailures` |
| `Final` drops the exit | `TestReporter_TheFinalBundleCarriesTheExit` |

Restore each and confirm the file is clean afterwards. `git checkout` cannot restore a new file, so check with `grep`.

- [ ] **Step 5: Commit**

---

### Task 3: Wire it into `run` and `serve`

**Files:**
- Modify: `internal/cli/run/root.go`, `internal/cli/run/metrics.go`
- Modify: `internal/cli/serve/serve.go`
- Test: `internal/cli/run/reporter_test.go`, `internal/cli/serve/reporter_test.go`

**Where each attaches:**

- **`run`**: after `newMeterProvider`, start `Run` on the run context. The final bundle goes in the existing deferred drain block in `root.go`, **after** the managers stop and the last `SyncState`, so the bundle reports state that is actually committed. Use the drain budget's context, so the reporter cannot outlive the deadline a supervisor is waiting on.
- **`serve`**: start it beside the server, and send the final bundle after `srv.Serve` returns and `srv.Close` has run.

Both read `instance.id` and the interval from their own config block, and both pass the same `Collect` closure their HTTP route uses. **One bundle builder, two transports** is the property the contract exists for, and a second builder here would let them drift.

- [ ] **Step 1: Write the failing test**

For `run`, the test starts a receiver, runs the command against a fixture config with `report_to` pointing at it and `--max-msgs 1`, and asserts: at least one bundle arrived, its `instance.id` is the configured one, and the last one carries `exit`.

For `serve`, the same shape using `serveConfig` with a `turbostats` block, cancelling the context to trigger the drain.

Both assert that the bundle's sections match the command: `pipeline` for `run`, `serve` for `serve`.

- [ ] **Step 2: Implement, then run with the race detector**

Run: `go build ./... && go vet ./... && go test -race -short ./internal/cli/...`

- [ ] **Step 3: Prove a failing receiver cannot hold a shutdown**

Point a config at a port nothing listens on, run with `--max-msgs 1`, and time it. The process must exit in about the time it takes to drain, not in ten seconds.

```bash
time ./bin/sqlflow run /tmp/deadend.yml --max-msgs 1
```

Paste the timing into the commit. This is the rule with the most operational weight: a reporter that delays a shutdown turns every deploy into a thirty-second wait.

- [ ] **Step 4: Commit**

---

### Task 4: The feature registry and the release test

**Files:**
- Modify: `docs/coverage/features.yml`, `tests/release/test_image.py`

- [ ] **Step 1: Register the feature**

```yaml
  - id: observability.turbostats.reporter
    description: Posts the process's own state to a control plane on an interval, signed.
    requires: [unit, release]
```

- [ ] **Step 2: Write the release test**

The shipped image must report. The test starts a tiny HTTP receiver the container can reach, runs the image with a config carrying a `turbostats` block, and asserts a signed bundle arrives whose `instance.version` matches the image tag.

Follow `test_turbostats_serve_bundle_carries_a_serve_section` for the container setup. The receiver is a `http.server` thread bound to `0.0.0.0` with the container on the host network, or a container on the same network — match whatever the existing Kafka tests do rather than inventing a third way.

**Do not verify the signature in Python.** Assert the three headers are present and well-formed; the signature is covered by the wire vectors and by Task 5 against the real control plane.

- [ ] **Step 3: Run it and regenerate the matrix**

```bash
make sqlflow-image && SQLFLOW_IMAGE=... uv run --locked pytest tests/release -k turbostats -v
make coverage-matrix && make coverage-check
```

- [ ] **Step 4: Commit**

---

### Task 5: Two repositories, one heartbeat

This is the task the whole contract exists for. Everything before it proves the engine agrees with itself.

- [ ] **Step 1: Run the real control plane**

```bash
cd ../sql-flow-control
make db-up && export DATABASE_URL='postgres://control:control@localhost:5433/control?sslmode=disable'
make build
./bin/sqlflow-control org create --slug turbolytics --name Turbolytics --public-read
./bin/sqlflow-control credential create --org turbolytics --name bluesky-host
./bin/sqlflow-control serve --addr 127.0.0.1:8090
```

- [ ] **Step 2: Point a real pipeline at it**

Add a `turbostats` block to a dev config with the credential in `SQLFLOW_TURBOSTATS_KEY`, and run it.

- [ ] **Step 3: Read the page**

Open `http://127.0.0.1:8090/o/turbolytics`. The instance must appear with its sections, a throughput figure, and a status of `up`.

Then stop the pipeline with SIGTERM and reload: the final bundle must make it `exited`, and the instance page's version history must show a clean stop rather than a crash.

**Capture all of it for the PR body**: the fleet row, the status before and after, and the control plane's log line accepting the bundle.

- [ ] **Step 4: Check the two failure paths by hand**

- **An unregistered key.** Revoke the credential and watch: the instance logs one warning, keeps running, and the page stops updating. Re-issue and register it, and the instance appears again with no restart.
- **A control plane that is down.** Stop it, watch the instance keep working, start it again, and confirm the page shows the gap.

- [ ] **Step 5: Commit the evidence, push, open the PR**

The PR body carries the captured output, names the four rules the reporter follows, and says what changes for `control.turbolytics.io`: the demo stops being fed by a signing helper.

---

## Self-review

**Spec coverage:**

| Spec | Task |
|---|---|
| The config block, `key`, `allow`, validation | 1 |
| Post at start, on the interval, with jitter | 2 |
| The final bundle on a clean drain | 2, 3 |
| Ten-second timeout, no retry, no queue | 2 |
| Any 2xx is success; `commands` ignored | 2 |
| A failure run logged at its edges | 2 |
| Plaintext to a non-loopback address refused | 1 |
| `--turbostats` and `report_to` independent | 3 |
| `observability.turbostats.reporter`, unit and release | 4 |
| The receiver actually accepts it | 5 |

**Deferred, and where each lands:** any command verb, command status and command signing are a later spec. Registering an operator-generated public key is a control-plane feature the contract already allows.

**Known risks, stated rather than hidden:**

- The release test needs the container to reach a receiver on the host, which is the fiddliest part of this plan. If the existing suite has no pattern for it, prefer a second container on the same network over a host-networking special case.
- `Final` runs inside the drain budget. If a supervisor's grace period is shorter than the post timeout, the bundle is lost and the stop reads as a crash. That is the correct trade — a delayed shutdown is worse — but it means a very short grace period makes every clean stop look unclean, and that is worth a line in the docs.
