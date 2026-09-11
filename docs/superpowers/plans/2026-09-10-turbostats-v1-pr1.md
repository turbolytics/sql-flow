# TurboStats v1, PR 1: the document and the endpoint

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A sqlflow process builds one versioned document about itself from the same instruments Prometheus reads, and serves it at `GET /turbostats/v1`.

**Architecture:** A new leaf package `internal/turbostats` owns the `Bundle` type and the one function that builds it, `Collect`, which reads an OTel manual reader plus the runtime and `/proc`. The run command attaches that manual reader to the meter provider always, attaches the Prometheus exporter only on request, and mounts the package's HTTP handler on the existing `:8000` mux behind a new `--turbostats` flag. Version and commit move to a leaf package so `run` can read them without importing `cli`.

**Tech Stack:** Go 1.25, OTel SDK metrics (`sdkmetric.ManualReader`, `metricdata`), cobra, `github.com/zeebo/assert`; pytest and testcontainers for the release test.

**Spec:** `docs/superpowers/specs/2026-09-10-turbostats-v1-design.md`. The reporter, the `pipeline.turbostats` config block, and the final bundle on drain are PR 2 and are not in this plan.

## Global Constraints

- The document is exactly the one in the spec's "The document" section. Field names are the OTel instrument names, never the Prometheus series names.
- Counters are totals since `process.started_at`. Gauges are the value now. Histograms are never in the bundle.
- `state_db_size_bytes` is omitted, not zero, when the pipeline has no state path.
- A bundle marshals to under 1 KiB. A test holds that bound.
- Timestamps are RFC 3339, UTC, whole seconds.
- Media type: `application/vnd.turbolytics.turbostats.v1+json`.
- `--turbostats` and `--metrics=prometheus` are independent. Either starts the HTTP server on `:8000`. Neither is on by default.
- Every new test carries `coverage.Covers(t, "observability.turbostats")` in Go, or `@pytest.mark.covers("observability.turbostats")` in Python, or the coverage gate reports the feature missing.
- All prose follows the repo's `CLAUDE.md`. Comments explain why. Commit messages name the defect, the fix and the evidence, and carry no attribution lines.
- Go tests run with `go test -short ./...`; the Python tooling suite with `uv run --locked pytest tests/tooling -q`. Commit after every task.

## How to read the code you will touch

- `internal/cli/run/root.go` is the run command. `RunE` loads the config, opens DuckDB, builds the meter provider through `newMeterProvider` in `metrics.go`, then the source, sink, handler and turbine. Flags are declared at the bottom of `NewCommand`.
- `internal/cli/run/metrics.go` builds the meter provider only for `--metrics=prometheus` and starts the HTTP server inside that branch. `newHTTPMux` registers `/metrics` and `/stats`.
- `internal/core/metrics.go` declares the pipeline's instruments by name. `internal/core/counting.go` declares `sink_rows_accepted` and `sink_rows_written`, each with attributes `sink` and `role`; the DLQ sink records with `role=dlq`.
- `internal/conformance/conformance.go`, function `deliveredRows`, is an existing example of reading a counter's total out of a `sdkmetric.ManualReader`. `Collect` generalizes it.
- `internal/handlers/leak_test.go`, function `residentAnonBytes`, already reads `RssAnon` from `/proc/self/status`. It moves.
- `tests/release/test_image.py` drives the shipped image. `test_lifecycle_drain_writes_the_buffered_batch_on_sigterm` is the model for starting the container detached against the `stack` fixture and waiting on a log line.

---

### Task 0: Start the record on the Bluesky host, today

No code, and not in this repository. The Bluesky instance's uptime clock is
running and nothing is recording it. History does not backfill, so this
happens before Task 1, not after Task 7.

**Files:** none in the repo. One crontab entry on the Bluesky host.

- [ ] **Step 1: Find the process**

On the host:

```bash
pgrep -f 'sqlflow run' | head -1
```

Expected: one PID. If the instance runs in Docker, use `docker inspect --format '{{.State.Pid}}' <container>` instead; the rest is the same.

- [ ] **Step 2: Add the cron line**

`crontab -e`, then:

```cron
* * * * * P=$(pgrep -f 'sqlflow run' | head -1); if [ -n "$P" ]; then echo "$(date -u +\%FT\%TZ) up $(ps -o lstart= -p $P | xargs -I{} date -u -d '{}' +\%FT\%TZ) $(ps -o rss= -p $P)" >> $HOME/sqlflow-uptime.log; else echo "$(date -u +\%FT\%TZ) down" >> $HOME/sqlflow-uptime.log; fi
```

Each line is: sample time, `up` with the process start time and RSS in KiB, or `down`. The `\%` escapes are cron's, not the shell's. On macOS `date -d` is `date -j -f`; the Bluesky host is assumed Linux.

- [ ] **Step 3: Confirm one line landed**

After a minute:

```bash
tail -2 $HOME/sqlflow-uptime.log
```

Expected: a line ending in a start time and a number. That file is what Task 6's status row and the control plane's store get backfilled from, and when Task 5 ships to this host the line changes to `curl -s localhost:8000/turbostats/v1 >> $HOME/sqlflow-turbostats.jsonl`.

---

### Task 1: Version and commit move to a leaf package

`run` cannot import `internal/cli` without a cycle, and the bundle needs the stamped version. A leaf package both can import fixes it. Three build scripts stamp the variables by fully qualified name and all three change.

**Files:**
- Create: `internal/buildinfo/buildinfo.go`
- Create: `internal/buildinfo/buildinfo_test.go`
- Modify: `internal/cli/version.go`
- Modify: `Makefile` lines 11-12
- Modify: `Dockerfile` line 42
- Modify: `scripts/release-binaries.sh` line 48

**Interfaces:**
- Produces: package `buildinfo` with `var Version = "dev"` and `var Commit = "unknown"`.

- [ ] **Step 1: Write the failing test**

Create `internal/buildinfo/buildinfo_test.go`:

```go
package buildinfo

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// An unstamped build must still identify itself, because a bundle from a
// developer's binary should say "dev" rather than an empty string.
func TestBuildInfo_UnstampedDefaults(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	assert.Equal(t, "dev", Version)
	assert.Equal(t, "unknown", Commit)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -short ./internal/buildinfo/`
Expected: build failure, `no Go files in .../internal/buildinfo` or `undefined: Version`.

- [ ] **Step 3: Create the package**

Create `internal/buildinfo/buildinfo.go`:

```go
// Package buildinfo carries what the build stamped into the binary.
//
// A leaf package on purpose: internal/cli prints these and internal/cli/run
// reports them in the TurboStats bundle, and run cannot import cli without a
// cycle. Both import this.
package buildinfo

// Version and Commit are stamped at link time by the Makefile, the Dockerfile
// and the release script:
//
//	go build -ldflags "-X github.com/turbolytics/sql-flow/internal/buildinfo.Version=v1.0.0"
//
// A plain `go build ./cmd/sqlflow/` leaves the defaults below, which is how an
// unreleased local binary identifies itself.
var (
	Version = "dev"
	Commit  = "unknown"
)
```

- [ ] **Step 4: Point `internal/cli/version.go` at it**

Replace the whole file with:

```go
package cli

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/buildinfo"
)

// Version and Commit live in internal/buildinfo, so the run command can
// report them without importing this package. These names stay exported
// because nothing outside the module should have to know that moved.
var (
	Version = buildinfo.Version
	Commit  = buildinfo.Commit
)

// versionString is what both `sqlflow version` and `sqlflow --version` print.
func versionString() string {
	return fmt.Sprintf(
		"sqlflow %s\ncommit: %s\ngo:     %s\n",
		buildinfo.Version, buildinfo.Commit, runtime.Version(),
	)
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the sqlflow version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprint(cmd.OutOrStdout(), versionString())
		},
	}
}
```

`versionString` reads `buildinfo` directly rather than the local copies, because `-X` sets `buildinfo.Version` at link time and the local `var` above is initialized from it in package-init order, which is fine for a print but is one more thing to reason about. The locals exist only so `cmd.Version = Version` in `root.go` keeps compiling.

- [ ] **Step 5: Change the three stamping sites**

In `Makefile`, lines 11-12:

```make
GO_LDFLAGS := -X $(GO_MODULE)/internal/buildinfo.Version=$(VERSION) \
	-X $(GO_MODULE)/internal/buildinfo.Commit=$(GIT_COMMIT)
```

In `Dockerfile`, line 42, replace both `internal/cli.` with `internal/buildinfo.`:

```
    -ldflags "-X github.com/turbolytics/sql-flow/internal/buildinfo.Version=${VERSION} -X github.com/turbolytics/sql-flow/internal/buildinfo.Commit=${COMMIT}" \
```

In `scripts/release-binaries.sh`, line 48:

```bash
LDFLAGS="-X ${GO_MODULE}/internal/buildinfo.Version=${VERSION} -X ${GO_MODULE}/internal/buildinfo.Commit=${COMMIT}"
```

- [ ] **Step 6: Run the tests and prove the stamp still lands**

Run: `go test -short ./internal/buildinfo/ ./internal/cli/`
Expected: PASS.

Run: `make sqlflow && ./bin/sqlflow version`
Expected: the first line is `sqlflow v1.1.0-N-gHASH` or similar from `git describe`, not `sqlflow dev`. If it prints `dev`, the Makefile path is wrong.

- [ ] **Step 7: Commit**

```bash
git add internal/buildinfo internal/cli/version.go Makefile Dockerfile scripts/release-binaries.sh
git commit -m "buildinfo: version and commit move to a leaf package

The TurboStats bundle reports the stamped version, and the run command
that builds it cannot import internal/cli without a cycle. Version and
Commit now live in internal/buildinfo, which both import. The three
stamping sites change with them; the release suite's version test is
what catches a site that was missed."
```

---

### Task 2: The rendered config text, and its hash

`config.Load` renders the template and throws the bytes away. The bundle's `config_hash` needs them. One new function returns both, and `Load` becomes a wrapper so nothing else changes.

**Files:**
- Modify: `internal/config/load.go` function `Load`
- Test: `internal/config/load_test.go`
- Create: `internal/turbostats/hash.go`
- Create: `internal/turbostats/hash_test.go`

**Interfaces:**
- Produces: `config.LoadRendered(path string, overrides map[string]string) (*Conf, []byte, error)`.
- Produces: `turbostats.HashConfig(rendered []byte) string`, returning `sha256:` plus 64 hex digits.

- [ ] **Step 1: Write the failing tests**

Append to `internal/config/load_test.go`:

```go
// The rendered text is what the TurboStats bundle hashes, so a config that
// renders differently under two environments hashes differently. Load used
// to discard it.
func TestLoadRendered_ReturnsTheTextTheConfWasParsedFrom(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	path := filepath.Join(t.TempDir(), "p.yml")
	src := "pipeline:\n  name: {{ SQLFLOW_NAME|default('demo') }}\n  source:\n    type: kafka\n  handler:\n    type: handlers.InferredMemBatch\n    sql: SELECT 1\n  sink:\n    type: noop\n"
	assert.NoError(t, os.WriteFile(path, []byte(src), 0o644))

	conf, rendered, err := LoadRendered(path, map[string]string{"SQLFLOW_NAME": "x"})
	assert.NoError(t, err)
	assert.Equal(t, "x", conf.Pipeline.Name)
	assert.That(t, strings.Contains(string(rendered), "name: x"))
	assert.That(t, !strings.Contains(string(rendered), "{{"))
}
```

Add `"os"`, `"path/filepath"` and `"strings"` to that file's imports if absent, and `"github.com/turbolytics/sql-flow/internal/coverage"`.

Create `internal/turbostats/hash_test.go`:

```go
package turbostats

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestHashConfig_IsSHA256OfTheRenderedText(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	got := HashConfig([]byte("pipeline:\n  name: x\n"))
	// sha256 of that exact text, computed once with
	// printf 'pipeline:\n  name: x\n' | shasum -a 256
	assert.Equal(t, "sha256:2d1b7510db148fa0dfa4e13e50f1bd30c4ccceeae5dc5f5f20cce0c8034c6fcd", got)
}

func TestHashConfig_DiffersWhenTheTextDoes(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	assert.That(t, HashConfig([]byte("a")) != HashConfig([]byte("b")))
	assert.Equal(t, 7+64, len(HashConfig([]byte("a"))))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/config/ ./internal/turbostats/`
Expected: `undefined: LoadRendered` and `no Go files` or `undefined: HashConfig`.

- [ ] **Step 3: Implement both**

In `internal/config/load.go`, replace `Load` with:

```go
// LoadRendered renders the file and parses it, returning both. The rendered
// text is what the TurboStats bundle hashes: two instances running the same
// file under different environments are running different configs, and the
// hash should say so.
func LoadRendered(path string, overrides map[string]string) (*Conf, []byte, error) {
	rendered, err := RenderTemplate(path, overrides)
	if err != nil {
		// Returned as-is. The inner error already names the file and the
		// stage that failed, so another "rendering config failed" prefix adds
		// a word and no information.
		return nil, nil, err
	}

	var conf Conf
	// Decoded strictly: the config schema sets additionalProperties: false, so
	// an unrecognized key is a typo the user wants to hear about rather than a
	// setting silently dropped.
	dec := yaml.NewDecoder(bytes.NewReader(rendered))
	dec.KnownFields(true)
	if err := dec.Decode(&conf); err != nil {
		return nil, nil, errs.Wrap(errs.CodeConfigParseFailed, err, "parsing YAML failed")
	}
	return &conf, rendered, nil
}

// Load is LoadRendered for callers that do not need the text.
func Load(path string, overrides map[string]string) (*Conf, error) {
	conf, _, err := LoadRendered(path, overrides)
	return conf, err
}
```

Create `internal/turbostats/hash.go`:

```go
package turbostats

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashConfig identifies a rendered config by content.
//
// Hashed after templating and before parsing, so the same file under two
// environments hashes differently: those are two configs. The prefix names
// the algorithm so a stored hash stays readable when the algorithm changes.
func HashConfig(rendered []byte) string {
	sum := sha256.Sum256(rendered)
	return "sha256:" + hex.EncodeToString(sum[:])
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -short ./internal/config/ ./internal/turbostats/`
Expected: PASS, including every existing `config` test, since `Load` still exists.

- [ ] **Step 5: Commit**

```bash
git add internal/config/load.go internal/config/load_test.go internal/turbostats/hash.go internal/turbostats/hash_test.go
git commit -m "config: Load keeps the rendered text, and turbostats hashes it

The bundle's config_hash identifies what an instance is running, and the
same file renders differently under two environments. Load rendered and
discarded the text. LoadRendered returns it beside the Conf, Load wraps
it, and HashConfig is sha256 with the algorithm named in the prefix."
```

---

### Task 3: The bundle, and the one function that builds it

**Files:**
- Create: `internal/turbostats/bundle.go`
- Create: `internal/turbostats/collect.go`
- Create: `internal/turbostats/collect_test.go`

**Interfaces:**
- Consumes: `HashConfig` is not used here; `core.StateStats` from `internal/core/stats.go` with field `SizeBytes int64`.
- Produces: types `Bundle`, `Instance`, `Process`, `Pipeline`, `Exit`, `Static`; `const MediaType`; `func Collect(ctx context.Context, s Static, r *sdkmetric.ManualReader, stats func() (*core.StateStats, error)) (Bundle, error)`.

One refinement to the spec surfaces here. The spec says an instrument with attributes is summed across them. `sink_rows_accepted` and `sink_rows_written` carry a `role` attribute, and the DLQ sink records with `role=dlq` precisely so its rows never sum into the pipeline's delivered series. Summing across `role` would undo that. So: sum across every attribute except `role`, where only `role=pipeline` counts. Task 7 adds that sentence to the spec.

- [ ] **Step 1: Write the failing tests**

Create `internal/turbostats/collect_test.go`:

```go
package turbostats

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// A provider with the engine's real instruments, driven a known distance.
func provider(t *testing.T) (*sdkmetric.ManualReader, *core.Metrics, metric.Meter) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)
	return reader, m, mp.Meter("sqlflow")
}

var static = Static{
	ID:         "pi-01",
	Pipeline:   "demo",
	Version:    "v1.2.3",
	Commit:     "abc1234",
	ConfigHash: "sha256:00",
	StartedAt:  time.Date(2026, 9, 1, 8, 12, 44, 0, time.UTC),
}

func TestCollect_CountersAreTotalsSinceStart(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.MessageCount.Add(ctx, 3)
	m.MessageCount.Add(ctx, 4)
	m.ErrorCount.Add(ctx, 1)

	b, err := Collect(ctx, static, reader, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), b.Pipeline.MessageCount)
	assert.Equal(t, int64(1), b.Pipeline.ErrorCount)
}

// consumer_lag is recorded per partition. The bundle carries one number.
func TestCollect_AGaugeIsSummedAcrossItsAttributes(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.ConsumerLag.Record(ctx, 5, metric.WithAttributes(attribute.Int("partition", 0)))
	m.ConsumerLag.Record(ctx, 7, metric.WithAttributes(attribute.Int("partition", 1)))

	b, err := Collect(ctx, static, reader, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(12), b.Pipeline.ConsumerLag)
}

// The DLQ records its rows with role=dlq so they never sum into the
// pipeline's delivered series. The bundle must not undo that.
func TestCollect_SinkRowsCountThePipelineRoleOnly(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, meter := provider(t)
	ctx := context.Background()
	written, err := meter.Int64Counter("sink_rows_written")
	assert.NoError(t, err)
	written.Add(ctx, 10, metric.WithAttributes(
		attribute.String("sink", "console"), attribute.String("role", "pipeline")))
	written.Add(ctx, 4, metric.WithAttributes(
		attribute.String("sink", "console"), attribute.String("role", "dlq")))

	b, err := Collect(ctx, static, reader, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), b.Pipeline.SinkRowsWritten)
}

func TestCollect_HistogramsAreNotInTheBundle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.SinkFlushLatency.Record(ctx, 0.25)

	b, err := Collect(ctx, static, reader, nil)
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "latency"))
	assert.That(t, !strings.Contains(string(raw), "bucket"))
}

// Absent state and empty state are different facts, the same rule /stats
// follows.
func TestCollect_StateSizeIsOmittedWithoutAStateDatabase(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), static, reader, nil)
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "state_db_size_bytes"))
}

func TestCollect_StateSizeComesFromTheStatsFunction(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	stats := func() (*core.StateStats, error) {
		return &core.StateStats{SizeBytes: 4096}, nil
	}

	b, err := Collect(context.Background(), static, reader, stats)
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.StateDBSizeBytes != nil)
	assert.Equal(t, int64(4096), *b.Pipeline.StateDBSizeBytes)
}

// A stats failure is the bundle's failure. A monitoring system must see it
// rather than a healthy-looking document with a field quietly missing.
func TestCollect_AStatsFailureFailsTheBundle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	stats := func() (*core.StateStats, error) { return nil, errors.New("unreadable") }

	_, err := Collect(context.Background(), static, reader, stats)
	assert.Error(t, err)
}

func TestCollect_CarriesTheStaticFactsAndTheRuntime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), static, reader, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, b.V)
	assert.Equal(t, "pi-01", b.Instance.ID)
	assert.Equal(t, "demo", b.Instance.Pipeline)
	assert.Equal(t, "v1.2.3", b.Instance.Version)
	assert.Equal(t, "abc1234", b.Instance.Commit)
	assert.Equal(t, "sha256:00", b.Instance.ConfigHash)
	assert.That(t, strings.Contains(b.Instance.Arch, "/"))
	assert.Equal(t, static.StartedAt, b.Process.StartedAt)
	assert.That(t, b.Process.Goroutines > 0)
	assert.That(t, b.Process.RSSBytes > 0)
	assert.That(t, b.Exit == nil)
	assert.That(t, !b.SentAt.IsZero())
	assert.Equal(t, 0, b.SentAt.Nanosecond())
}

func TestCollect_TheBundleIsUnderOneKiB(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, meter := provider(t)
	ctx := context.Background()
	// Nine-digit totals, the size a year of the Bluesky firehose reaches.
	m.MessageCount.Add(ctx, 184203311)
	m.HandlerRowsRead.Add(ctx, 184203311)
	m.SinkFlushCount.Add(ctx, 1842033)
	m.StateCommitCount.Add(ctx, 1842033)
	for _, name := range []string{"sink_rows_accepted", "sink_rows_written"} {
		c, err := meter.Int64Counter(name)
		assert.NoError(t, err)
		c.Add(ctx, 184203311, metric.WithAttributes(
			attribute.String("sink", "clickhouse"), attribute.String("role", "pipeline")))
	}
	size := int64(4194304)
	stats := func() (*core.StateStats, error) { return &core.StateStats{SizeBytes: size}, nil }

	b, err := Collect(ctx, static, reader, stats)
	assert.NoError(t, err)
	b.Exit = &Exit{Reason: "SIGTERM", Code: 0}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, len(raw) < 1024)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/turbostats/`
Expected: `undefined: Static`, `undefined: Collect`.

- [ ] **Step 3: Write the types**

Create `internal/turbostats/bundle.go`:

```go
// Package turbostats is the document a sqlflow process reports about itself.
//
// One bundle, built by one function, carried by two transports: the HTTP
// handler in this package serves it to whatever can reach the process, and
// the reporter posts it outbound to a control plane that cannot. Neither
// transport computes anything.
//
// Field names are the OTel instrument names in internal/core, not the
// Prometheus series names those instruments render as. The bundle reads the
// instrument; the suffixes belong to one exporter.
package turbostats

import "time"

// MediaType names the version on the wire, so v1 and v2 are distinguishable
// without parsing.
const MediaType = "application/vnd.turbolytics.turbostats.v1+json"

// Version is the document version, carried in the body so a bundle stored or
// forwarded without its URL still says what it is.
const Version = 1

// Bundle is one report. The spec at
// docs/superpowers/specs/2026-09-10-turbostats-v1-design.md defines every
// field; the comments here say only what is not obvious from the name.
type Bundle struct {
	V        int       `json:"v"`
	SentAt   time.Time `json:"sent_at"`
	Instance Instance  `json:"instance"`
	Process  Process   `json:"process"`
	Pipeline Pipeline  `json:"pipeline"`
	// Exit is present only in the last bundle a clean shutdown sends. A
	// bundle without it is an instance still running, or one that died
	// without saying so.
	Exit *Exit `json:"exit,omitempty"`
}

// Instance is what the operator and the build said this process is.
type Instance struct {
	// ID is the operator's name for the instance. Empty until the reporter
	// config exists, which is why it is omitempty here and required there.
	ID         string `json:"id,omitempty"`
	Pipeline   string `json:"pipeline,omitempty"`
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	Arch       string `json:"arch"`
	ConfigHash string `json:"config_hash"`
}

// Process is what the runtime and the kernel say about this process.
type Process struct {
	StartedAt  time.Time `json:"started_at"`
	RSSBytes   int64     `json:"rss_bytes"`
	Goroutines int       `json:"goroutines"`
}

// Pipeline carries one field per counter or dimensionless gauge the engine
// declares. Counters are totals since Process.StartedAt; gauges are the value
// now. Histograms are deliberately absent: they are most of a scrape by bytes
// and nothing on the first page reads them.
type Pipeline struct {
	MessageCount     int64 `json:"message_count"`
	HandlerRowsRead  int64 `json:"handler_rows_read"`
	ErrorCount       int64 `json:"error_count"`
	SinkFlushCount   int64 `json:"sink_flush_count"`
	SinkRowsAccepted int64 `json:"sink_rows_accepted"`
	SinkRowsWritten  int64 `json:"sink_rows_written"`
	StateCommitCount int64 `json:"state_commit_count"`
	ConsumerLag      int64 `json:"consumer_lag"`
	// A pointer so a pipeline with no state path omits the field: absent
	// state and empty state are different facts.
	StateDBSizeBytes *int64 `json:"state_db_size_bytes,omitempty"`
}

// Exit is how a clean shutdown ended.
type Exit struct {
	Reason string `json:"reason"`
	Code   int    `json:"code"`
}

// Static is what the run command knows once, at startup, and the package
// cannot learn on its own.
type Static struct {
	ID, Pipeline, Version, Commit, ConfigHash string
	StartedAt                                 time.Time
}
```

- [ ] **Step 4: Write `Collect`**

Create `internal/turbostats/collect.go`:

```go
package turbostats

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Collect builds one bundle. It is the only function that does.
//
// It allocates one bundle and touches nothing shared, so the HTTP handler and
// the reporter can call it at once. stats may be nil for a pipeline with no
// state path; a stats error is the bundle's error, because a document with a
// field quietly missing reads as healthy.
func Collect(ctx context.Context, s Static, r *sdkmetric.ManualReader,
	stats func() (*core.StateStats, error)) (Bundle, error) {

	var rm metricdata.ResourceMetrics
	if err := r.Collect(ctx, &rm); err != nil {
		return Bundle{}, fmt.Errorf("turbostats: collecting instruments: %w", err)
	}
	totals := sumByName(rm)

	rss, err := ResidentAnonBytes()
	if err != nil {
		return Bundle{}, fmt.Errorf("turbostats: reading resident memory: %w", err)
	}

	b := Bundle{
		V:      Version,
		SentAt: time.Now().UTC().Truncate(time.Second),
		Instance: Instance{
			ID:         s.ID,
			Pipeline:   s.Pipeline,
			Version:    s.Version,
			Commit:     s.Commit,
			Arch:       runtime.GOOS + "/" + runtime.GOARCH,
			ConfigHash: s.ConfigHash,
		},
		Process: Process{
			StartedAt:  s.StartedAt.UTC().Truncate(time.Second),
			RSSBytes:   rss,
			Goroutines: runtime.NumGoroutine(),
		},
		Pipeline: Pipeline{
			MessageCount:     totals["message_count"],
			HandlerRowsRead:  totals["handler_rows_read"],
			ErrorCount:       totals["error_count"],
			SinkFlushCount:   totals["sink_flush_count"],
			SinkRowsAccepted: totals["sink_rows_accepted"],
			SinkRowsWritten:  totals["sink_rows_written"],
			StateCommitCount: totals["state_commit_count"],
			ConsumerLag:      totals["consumer_lag"],
		},
	}

	if stats != nil {
		st, err := stats()
		if err != nil {
			return Bundle{}, fmt.Errorf("turbostats: reading state stats: %w", err)
		}
		if st != nil {
			size := st.SizeBytes
			b.Pipeline.StateDBSizeBytes = &size
		}
	}
	return b, nil
}

// roleKey is the attribute the sink counters carry to keep the DLQ's rows out
// of the pipeline's delivered series. The bundle keeps them out too.
var roleKey = attribute.Key("role")

// sumByName folds every int64 counter and gauge into one total per name,
// summed across attribute sets, except that a point with a role other than
// "pipeline" is skipped. Histograms and float instruments are ignored: the
// bundle carries none.
func sumByName(rm metricdata.ResourceMetrics) map[string]int64 {
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if pipelineRole(dp.Attributes) {
						out[m.Name] += dp.Value
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if pipelineRole(dp.Attributes) {
						out[m.Name] += dp.Value
					}
				}
			}
		}
	}
	return out
}

func pipelineRole(set attribute.Set) bool {
	v, ok := set.Value(roleKey)
	return !ok || v.AsString() == "pipeline"
}
```

`ResidentAnonBytes` does not exist yet; Task 4 creates it. To make this task's tests runnable on their own, create it now in its final form as part of this task (Task 4 then only moves the leak test onto it). Create `internal/turbostats/rss.go`:

```go
package turbostats

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// ResidentAnonBytes is the process's anonymous resident memory.
//
// That is the figure a native leak moves: Go's heap profiler cannot see a
// buffer DuckDB allocated across the ADBC boundary, and duckdb_memory() does
// not track it either, so the only honest instrument is the process itself.
//
// Linux reads RssAnon, which excludes file-backed pages and is exact. Other
// platforms fall back to peak resident size from getrusage, which only ever
// rises, so it is a weaker signal there; a leak still shows as growth between
// two readings, since a steady process has a steady peak.
func ResidentAnonBytes() (int64, error) {
	if runtime.GOOS == "linux" {
		f, err := os.Open("/proc/self/status")
		if err != nil {
			return 0, err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "RssAnon:") {
				continue
			}
			fields := strings.Fields(line)
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return kb << 10, nil
		}
		return 0, fmt.Errorf("RssAnon not found in /proc/self/status")
	}
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, err
	}
	maxrss := int64(ru.Maxrss)
	if runtime.GOOS != "darwin" {
		maxrss <<= 10 // kilobytes everywhere but darwin, which reports bytes
	}
	return maxrss, nil
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -short ./internal/turbostats/`
Expected: PASS, all nine.

- [ ] **Step 6: Commit**

```bash
git add internal/turbostats/bundle.go internal/turbostats/collect.go internal/turbostats/rss.go internal/turbostats/collect_test.go
git commit -m "turbostats: the bundle, and Collect, the one function that builds it

A process reports its own state as one versioned document: what it is,
when it started, its resident memory, and every counter's total since
start, read from the same instruments Prometheus reads. Histograms are
out, so the bundle stays under 1 KiB, and a test holds it there.

The sink row counters carry role=dlq for the DLQ so its rows never sum
into the delivered series. Summing across attributes blindly would have
undone that; a test now proves the bundle counts the pipeline role only."
```

---

### Task 4: The leak test reads memory through the package

The reading moved. The test that made it should use the moved copy, so there is one implementation and not two that drift.

**Files:**
- Modify: `internal/handlers/leak_test.go` lines 18-59

- [ ] **Step 1: Replace the helper with a wrapper**

In `internal/handlers/leak_test.go`, delete the `residentAnonBytes` function and its comment (lines 18-59) and replace with:

```go
// residentAnonBytes is turbostats.ResidentAnonBytes, which moved there so
// the bundle and this test read the same number, with the test's failure
// semantics.
func residentAnonBytes(t *testing.T) int64 {
	t.Helper()
	n, err := turbostats.ResidentAnonBytes()
	if err != nil {
		t.Fatal(err)
	}
	return n
}
```

Add `"github.com/turbolytics/sql-flow/internal/turbostats"` to the imports, and remove `"bufio"`, `"os"`, `"strconv"`, `"strings"` and `"syscall"` if nothing else in the file uses them. `"runtime"` stays for `settle`.

- [ ] **Step 2: Run the package's tests**

Run: `go test -short ./internal/handlers/ -run 'Leak|Release'`
Expected: PASS. `TestInferredInvoke_DoesNotLeakNativeMemory` logs the same `resident anon memory:` line it did before.

Run: `go vet ./internal/handlers/`
Expected: clean. An unused import is the likely failure.

- [ ] **Step 3: Commit**

```bash
git add internal/handlers/leak_test.go
git commit -m "handlers: the leak test reads memory through turbostats

One implementation of the RssAnon read, not two that drift. The test
keeps its t.Fatal semantics in a three-line wrapper."
```

---

### Task 5: The handler, the always-present reader, and the flag

**Files:**
- Create: `internal/turbostats/handler.go`
- Create: `internal/turbostats/handler_test.go`
- Modify: `internal/cli/run/metrics.go` (`newHTTPMux`, `newMeterProvider`)
- Modify: `internal/cli/run/metrics_test.go` (four `newHTTPMux` call sites)
- Modify: `internal/cli/run/root.go` (flag, `started_at`, `LoadRendered`, `Static`, the provider call)

**Interfaces:**
- Consumes: `turbostats.Collect`, `turbostats.Static`, `turbostats.MediaType`, `config.LoadRendered`, `turbostats.HashConfig`, `buildinfo.Version`, `buildinfo.Commit`.
- Produces: `turbostats.Handler(collect func(context.Context) (Bundle, error)) http.Handler`.
- Produces: `newHTTPMux(registry *prom.Registry, stats statsFunc, collect func(context.Context) (turbostats.Bundle, error)) *http.ServeMux`.
- Produces: `newMeterProvider(exporter string, serveTurbostats bool, static turbostats.Static, l *zap.Logger, stats statsFunc) (metric.MeterProvider, error)`.

- [ ] **Step 1: Write the failing handler tests**

Create `internal/turbostats/handler_test.go`:

```go
package turbostats

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestHandler_ServesOneBundleWithTheMediaType(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	h := Handler(func(context.Context) (Bundle, error) {
		return Bundle{V: Version, Instance: Instance{Version: "v9"}}, nil
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, MediaType, rec.Header().Get("Content-Type"))
	var got Bundle
	assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, 1, got.V)
	assert.Equal(t, "v9", got.Instance.Version)
}

// A collection failure is a server error, not an empty success: a monitoring
// system must see it rather than a healthy-looking blank.
func TestHandler_ReportsACollectionFailure(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	h := Handler(func(context.Context) (Bundle, error) {
		return Bundle{}, errors.New("reader closed")
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandler_RejectsAnythingButGET(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	h := Handler(func(context.Context) (Bundle, error) { return Bundle{V: Version}, nil })

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/turbostats/ -run Handler`
Expected: `undefined: Handler`.

- [ ] **Step 3: Write the handler**

Create `internal/turbostats/handler.go`:

```go
package turbostats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// Handler serves one bundle per GET. The run command mounts it at
// /turbostats/v1; the path is the caller's, the media type is this
// package's.
func Handler(collect func(context.Context) (Bundle, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		b, err := collect(r.Context())
		if err != nil {
			http.Error(w, fmt.Sprintf("building bundle: %v", err),
				http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", MediaType)
		if err := json.NewEncoder(w).Encode(b); err != nil {
			return
		}
	})
}
```

- [ ] **Step 4: Run the handler tests**

Run: `go test -short ./internal/turbostats/`
Expected: PASS.

- [ ] **Step 5: Restructure the meter provider and the mux**

In `internal/cli/run/metrics.go`, add to the imports:

```go
	"context"

	"github.com/turbolytics/sql-flow/internal/turbostats"
```

Replace `newHTTPMux` with:

```go
// newHTTPMux builds the server the pipeline exposes: Prometheus scraping, a
// JSON view of durable state, and the TurboStats bundle.
//
// All on one mux deliberately. DuckDB takes an exclusive lock on the state
// file, so no other process can read it while the pipeline runs -- not even
// read-only. A running pipeline is the only thing that can report its own
// state, which makes these the live half of observability rather than a
// convenience.
//
// Each route is registered only when its provider is non-nil, so a route that
// would answer with nothing is absent rather than half-working.
func newHTTPMux(registry *prom.Registry, stats statsFunc,
	collect func(context.Context) (turbostats.Bundle, error)) *http.ServeMux {
	mux := http.NewServeMux()

	if registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	}

	if stats != nil {
		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			state, err := stats()
			if err != nil {
				// A monitoring system must see the failure, not a
				// healthy-looking blank.
				http.Error(w, fmt.Sprintf("collecting state stats: %v", err),
					http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			// state is nil for a pipeline with no state database, which
			// encodes as null: absent state and empty state are different
			// facts and a dashboard should be able to tell them apart.
			if err := json.NewEncoder(w).Encode(map[string]any{"state": state}); err != nil {
				return
			}
		})
	}

	if collect != nil {
		mux.Handle("/turbostats/v1", turbostats.Handler(collect))
	}

	return mux
}
```

Replace `newMeterProvider` with:

```go
// newMeterProvider builds the provider every instrument records into, and
// starts the HTTP server when anything needs it.
//
// The manual reader is attached always: it is what /turbostats/v1 reads, and
// the outbound reporter after it. The Prometheus exporter is a second reader,
// attached only for --metrics=prometheus. Both read the same instruments,
// which is the property the design exists for. Before this, the provider
// existed only for Prometheus and every counter recorded into nothing without
// it.
func newMeterProvider(exporter string, serveTurbostats bool,
	static turbostats.Static, l *zap.Logger, stats statsFunc) (metric.MeterProvider, error) {

	reader := sdkmetric.NewManualReader()
	opts := []sdkmetric.Option{sdkmetric.WithReader(reader)}

	var registry *prom.Registry
	switch strings.ToLower(strings.TrimSpace(exporter)) {
	case "":
	case "prometheus":
		registry = prom.NewRegistry()
		exp, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, fmt.Errorf("prometheus exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(exp))
	default:
		return nil, fmt.Errorf("unsupported --metrics exporter: %q (supported: prometheus)", exporter)
	}

	mp := sdkmetric.NewMeterProvider(opts...)

	if registry == nil && !serveTurbostats {
		return mp, nil
	}

	var collect func(context.Context) (turbostats.Bundle, error)
	if serveTurbostats {
		collect = func(ctx context.Context) (turbostats.Bundle, error) {
			return turbostats.Collect(ctx, static, reader, stats)
		}
	}
	mux := newHTTPMux(registry, stats, collect)

	go func() {
		routes := []string{}
		if registry != nil {
			routes = append(routes, "/metrics")
		}
		if serveTurbostats {
			routes = append(routes, "/turbostats/v1")
		}
		l.Info("serving http", zap.String("addr", metricsPort), zap.Strings("routes", routes))
		if err := http.ListenAndServe(metricsPort, mux); err != nil {
			l.Error("http server stopped", zap.Error(err))
		}
	}()

	return mp, nil
}
```

- [ ] **Step 6: Update the four existing mux tests**

In `internal/cli/run/metrics_test.go`, every `newHTTPMux(nil, ...)` call gains a third argument `nil`. Four sites: lines 38, 65, 81 and 94 become

```go
	mux := newHTTPMux(nil, func() (*core.StateStats, error) { return want, nil }, nil)
```

and the equivalent for the other three.

Then append one test:

```go
// /turbostats/v1 is present only when asked for, like /stats: a route that
// answers with nothing is worse than none.
func TestObservabilityTurbostats_RouteAbsentWithoutACollector(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	mux := newHTTPMux(nil, nil, nil)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestObservabilityTurbostats_RouteServesTheBundle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	mux := newHTTPMux(nil, nil, func(context.Context) (turbostats.Bundle, error) {
		return turbostats.Bundle{V: turbostats.Version}, nil
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, turbostats.MediaType, rec.Header().Get("Content-Type"))
}

// Attaching the manual reader must not change what Prometheus exports. The
// series list is what the README documents and dashboards depend on.
func TestObservabilityTurbostats_ManualReaderLeavesExportedNamesAlone(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	coverage.Covers(t, "observability.metrics")
	reg := prom.NewRegistry()
	exp, err := prometheus.New(prometheus.WithRegisterer(reg))
	assert.NoError(t, err)
	manual := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(manual), sdkmetric.WithReader(exp))

	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)
	m.MessageCount.Add(context.Background(), 1)

	families, err := reg.Gather()
	assert.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "message_count_messages_total" {
			found = true
		}
	}
	assert.That(t, found)
}
```

Add `"github.com/turbolytics/sql-flow/internal/turbostats"` to that file's imports.

- [ ] **Step 7: Wire the run command**

In `internal/cli/run/root.go`:

Add to the imports:

```go
	"github.com/turbolytics/sql-flow/internal/buildinfo"
	"github.com/turbolytics/sql-flow/internal/turbostats"
```

In `NewCommand`, after `var withHTTPDebug bool` add:

```go
	var serveTurbostats bool
```

At the top of `RunE`, immediately after the logger is built (after `l := logger.Named("sqlflow.run")` and its error check), add:

```go
			// Taken here, once. It is the bundle's started_at, and a restart
			// is the control plane seeing it change.
			startedAt := time.Now().UTC()
```

Replace the config load:

```go
			conf, rendered, err := config.LoadRendered(configPath, map[string]string{})
			if err != nil {
				// Returned as-is: the error already names the file, the stage and the
				// code, so another prefix adds a word and no information.
				return err
			}
```

Replace the `newMeterProvider` call:

```go
			static := turbostats.Static{
				Pipeline:   conf.Pipeline.Name,
				Version:    buildinfo.Version,
				Commit:     buildinfo.Commit,
				ConfigHash: turbostats.HashConfig(rendered),
				StartedAt:  startedAt,
			}
			meterProvider, err := newMeterProvider(metricsExporter, serveTurbostats, static, l, statsFn)
			if err != nil {
				return err
			}
```

`static.ID` stays empty in this PR. The `pipeline.turbostats` config block that supplies it is PR 2.

At the bottom of `NewCommand`, after the `--with-http-debug` flag, add:

```go
	cmd.Flags().BoolVar(&serveTurbostats, "turbostats", false,
		"Serve GET /turbostats/v1 on "+metricsPort+": the process's own state as one document")
```

- [ ] **Step 8: Build, vet, and run the affected packages**

Run: `go build ./... && go vet ./internal/cli/... ./internal/turbostats/`
Expected: clean.

Run: `go test -short ./internal/cli/... ./internal/turbostats/ ./internal/core/`
Expected: PASS, including `TestExportedSeriesNames`, which proves the Prometheus series list did not change.

Run: `make sqlflow && ./bin/sqlflow run --help | grep turbostats`
Expected: the flag and its help text.

- [ ] **Step 9: Prove it end to end against a live process**

The Bluesky raw config needs only the network. From the repo root:

```bash
./bin/sqlflow run dev/config/examples/bluesky/bluesky.raw.stdout.yml --turbostats > /dev/null 2>&1 &
PID=$!
sleep 5
curl -s -D - http://localhost:8000/turbostats/v1
kill $PID
```

Expected: `Content-Type: application/vnd.turbolytics.turbostats.v1+json`, a body with `"v":1`, `"version"` matching `./bin/sqlflow version`, a `started_at` a few seconds ago, `message_count` above zero, and no `state_db_size_bytes` since that config has no state path. If the machine has no network, use any Kafka example against `make start-backing-services` instead; the assertion is the same.

- [ ] **Step 10: Commit**

```bash
git add internal/turbostats/handler.go internal/turbostats/handler_test.go internal/cli/run/metrics.go internal/cli/run/metrics_test.go internal/cli/run/root.go
git commit -m "turbostats: GET /turbostats/v1, behind --turbostats

The meter provider existed only for --metrics=prometheus, so without it
every counter recorded into nothing and there was nothing for a bundle
to read. The manual reader is now attached always, and the Prometheus
exporter is a second reader on request. A test holds the exported
series list unchanged, so attaching the reader cost no dashboard.

--turbostats starts the HTTP server without Prometheus and mounts the
bundle at /turbostats/v1 with its media type. Default stays off: a
device should not listen on a port unless told to."
```

---

### Task 6: The feature registry, and the release test

**Files:**
- Modify: `docs/coverage/features.yml` after the `observability.debug_api` entry
- Modify: `docs/coverage/status/features.yml`
- Modify: `docs/coverage/matrix.md` (regenerated)
- Modify: `tests/release/test_image.py`

- [ ] **Step 1: Declare the feature**

In `docs/coverage/features.yml`, after the `observability.debug_api` entry, add:

```yaml
  - id: observability.turbostats
    description: Reports the process's own state as one versioned document, served at /turbostats/v1.
    requires: [unit, release]
```

- [ ] **Step 2: Write the failing release test**

Append to `tests/release/test_image.py`:

```python
@pytest.mark.covers("observability.turbostats")
def test_turbostats_endpoint_serves_the_bundle(image, stack):
    """The shipped image answers /turbostats/v1 with a bundle whose version
    is the one stamped into the image.

    Driven against the real binary rather than the handler, because what
    ships is the flag, the mux and the link-time stamp together, and a unit
    test proves none of those.
    """
    topic = f"turbostats-{int(time.time())}"

    container = DockerContainer(image) \
        .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
        .with_env("SQLFLOW_KAFKA_BROKERS", "kafka:9092") \
        .with_env("SQLFLOW_TOPIC", topic) \
        .with_env("SQLFLOW_GROUP_ID", topic) \
        .with_exposed_ports(8000) \
        .with_network(stack.network) \
        .with_command("run /tmp/conf/config/examples/kafka.stateful.window.yml --turbostats")
    container.start()
    try:
        wait_for_logs(container, "consumer loop starting", timeout=60)
        port = container.get_exposed_port(8000)
        resp = requests.get(f"http://localhost:{port}/turbostats/v1", timeout=10)
    finally:
        container.stop()

    assert resp.status_code == 200
    assert resp.headers["Content-Type"] == "application/vnd.turbolytics.turbostats.v1+json"
    bundle = resp.json()
    assert bundle["v"] == 1

    stdout, _ = run_docker_container(image, "version")
    stamped = stdout.splitlines()[0].split()[1]
    assert bundle["instance"]["version"] == stamped
    assert bundle["process"]["rss_bytes"] > 0
    assert bundle["process"]["goroutines"] > 0
```

`requests` is already imported at the top of `tests/release/test_image.py` and pinned in `pyproject.toml`, so nothing is added.

- [ ] **Step 3: Run the release test**

Run: `make sqlflow-image && SQLFLOW_IMAGE=turbolytics/sql-flow:$(git describe --tags --always --dirty) SQLFLOW_PYTEST_JSON=$(pwd)/.coverage/pytest.json TC_KAFKA_LIMIT_BROKER_TO_FIRST_HOST=true uv run --locked pytest tests/release -q -k turbostats`
Expected: PASS. If it fails on the version assertion, print `stdout` from `run_docker_container(image, "version")`; the Dockerfile stamp from Task 1 is the likely cause.

- [ ] **Step 4: Regenerate the coverage page and add the status row**

The status directory is regenerated from test reports in CI. Locally, add the row by hand where it will land, sorted by id, in `docs/coverage/status/features.yml` after `observability.metrics`:

```yaml
observability.turbostats: {unit: covered, integration: not_required, release: covered}
```

Then:

```bash
make coverage-page
git status --short docs/coverage
```

Expected: `matrix.md` modified with one new feature row and nothing else. If CI's coverage job later reports the status file stale, the diff it prints is the correction.

- [ ] **Step 5: Run the tooling suite**

Run: `uv run --locked pytest tests/tooling -q`
Expected: PASS. `test_the_committed_page_is_current` proves the page matches the status files and registries.

- [ ] **Step 6: Commit**

```bash
git add docs/coverage/features.yml docs/coverage/status/features.yml docs/coverage/matrix.md tests/release/test_image.py
git commit -m "coverage: observability.turbostats, proven at unit and release

The feature is declared, so the gate can hold it to a unit pass and a
release pass. The release test starts the shipped image with
--turbostats and checks the bundle's version against what the image
prints, which is what proves the flag, the mux and the link-time stamp
shipped together."
```

---

### Task 7: The spec refinement, final verification, push

**Files:**
- Modify: `docs/superpowers/specs/2026-09-10-turbostats-v1-design.md`, the `pipeline` paragraph under "Fields"

- [ ] **Step 1: Record two refinements in the spec**

In the spec's "The package" section, the `Static` type gains the start time, since it is a fact the run command knows once and the package cannot learn. Replace

```go
type Static struct {
    ID, Pipeline, Version, Commit, ConfigHash string
}
```

with

```go
type Static struct {
    ID, Pipeline, Version, Commit, ConfigHash string
    StartedAt                                 time.Time
}
```

Then, in the paragraph beginning "`pipeline` holds one field per instrument", replace the sentence

```
An instrument that carries attributes is summed across them, so
`consumer_lag` is the sum over partitions.
```

with

```
An instrument that carries attributes is summed across them, so
`consumer_lag` is the sum over partitions, with one exception: a data point
whose `role` attribute is not `pipeline` is skipped. The DLQ sink records
`sink_rows_accepted` and `sink_rows_written` with `role=dlq` so its rows
never sum into the pipeline's delivered series, and the bundle keeps them
out the same way.
```

- [ ] **Step 2: Run everything**

```bash
gofmt -l internal/ cmd/
go build ./... && go vet ./...
go test -short -race ./...
uv run --locked pytest tests/tooling -q
make coverage-page && git status --short
```

Expected: gofmt prints nothing; build and vet clean; every Go package `ok`; the tooling suite passes; the tree is clean after `coverage-page`.

- [ ] **Step 3: Confirm the spec's PR 1 done-when**

Run: `curl -s http://localhost:8000/turbostats/v1 | wc -c` against the process from Task 5 Step 9.
Expected: under 1024.

- [ ] **Step 4: Commit and push**

```bash
git add docs/superpowers/specs/2026-09-10-turbostats-v1-design.md
git commit -m "spec: the bundle counts the pipeline role only

Summing sink rows across every attribute would fold the DLQ's rows into
the delivered series, which role=dlq exists to prevent. Found by the
test that holds Collect to it."
git push -u origin HEAD
```

Then open the PR. The body should say what the Bluesky host operator does next: deploy the image, start it with `--turbostats`, and switch the cron from `ps` to `curl http://localhost:8000/turbostats/v1`.
