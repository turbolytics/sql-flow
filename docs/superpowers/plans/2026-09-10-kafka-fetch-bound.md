# Kafka Source Read-Ahead Bound Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound how far the Kafka source reads ahead of the pipeline, with a measured default, so a backlog replay peaks at a configured size instead of the size of the backlog.

**Architecture:** A `fetch` block on the kafka source carries franz-go's two fetch sizes and the depth of the channel between the poll goroutine and the pipeline. `internal/sources/init.go` resolves the block to defaults and passes the values through. The source and the consume loop are unchanged: a full channel already blocks the poll goroutine, and franz-go already stops fetching from a broker whose last fetch is unpolled.

**Tech Stack:** Go 1.25, franz-go v1.20.7 (`kgo`), yaml.v3 strict decoding, generated JSON schema in `internal/validate/schemas/config.json`, testcontainers Kafka for the integration pass, `scripts/benchmark-container.sh` for the measurements.

**Spec:** `docs/superpowers/specs/2026-09-10-kafka-fetch-bound-design.md`

## Global Constraints

- Work in the worktree `.claude/worktrees/kafka-fetch-bound` on branch `feat/kafka-fetch-bound`. Never `git checkout` in the main checkout while a benchmark runs.
- Every Go test starts with `coverage.Covers(t, "<feature id>")` as its first statement. A test that skips covers nothing. Feature ids used here: `source.kafka`, `config.templating`, `config.validation`.
- Integration tests are named `TestIntegration<Feature>_<Behaviour>` and must never skip except under `-short`.
- The JSON schema is generated. After any change to `internal/config/config.go`, run `make schema` and commit `internal/validate/schemas/config.json`. `TestConfigSchema_CommittedFileMatchesTheTypes` fails until it is regenerated.
- Prose in comments, commit messages, the PR, and docs follows Google Technical Writing One. No em dashes anywhere. No attribution lines in commits or the PR.
- Python runs through `uv`: hand the benchmark script a wrapper that execs `uv run --project <repo> python`. Never bare `python3`.
- Defaults must equal today's fetch sizes: `max_bytes` 104857600, `max_partition_bytes` 10485760. Only `prefetch` changes behavior.
- The coverage matrix (`docs/coverage/matrix.json`, `matrix.md`) is regenerated from CI artifacts only, never from a local run.

---

### Task 1: The `fetch` block in config, with defaults and validation

**Files:**
- Modify: `internal/config/config.go:163-172` (the `KafkaSource` struct)
- Test: `internal/config/kafka_fetch_test.go` (new)
- Regenerate: `internal/validate/schemas/config.json`

**Interfaces:**
- Consumes: nothing new.
- Produces: `config.KafkaFetch{MaxBytes, MaxPartitionBytes, Prefetch int}`, the field `KafkaSource.Fetch *KafkaFetch`, the method `func (f *KafkaFetch) Resolved() (KafkaFetch, error)` valid on a nil receiver, and the constants `config.DefaultKafkaFetchMaxBytes`, `config.DefaultKafkaFetchMaxPartitionBytes`, `config.DefaultKafkaFetchPrefetch`.

- [ ] **Step 1: Write the failing tests**

Create `internal/config/kafka_fetch_test.go`:

```go
package config

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

const kafkaFetchConfig = `
pipeline:
  batch_size: 10
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: g
      auto_offset_reset: earliest
      topics: [t]
      fetch:
        max_bytes: 52428800
        max_partition_bytes: 1048576
        prefetch: 4
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: noop
`

// The block is optional. Every field it carries is read as written.
func TestConfigTemplating_Load_KafkaFetchBlock(t *testing.T) {
	coverage.Covers(t, "config.templating")
	conf := loadString(t, kafkaFetchConfig)

	f := conf.Pipeline.Source.Kafka.Fetch
	assert.NotNil(t, f)
	assert.Equal(t, 52428800, f.MaxBytes)
	assert.Equal(t, 1048576, f.MaxPartitionBytes)
	assert.Equal(t, 4, f.Prefetch)

	resolved, err := f.Resolved()
	assert.NoError(t, err)
	assert.Equal(t, *f, resolved)
}

// An absent block, or an absent field, is the default. The two byte defaults
// are the values sources/init.go set before the block existed; only prefetch
// is new, and it is what bounds a backlog replay.
func TestConfigTemplating_Load_KafkaFetchAbsentIsTheDefault(t *testing.T) {
	coverage.Covers(t, "config.templating")

	var absent *KafkaFetch
	resolved, err := absent.Resolved()
	assert.NoError(t, err)
	assert.Equal(t, KafkaFetch{
		MaxBytes:          100 << 20,
		MaxPartitionBytes: 10 << 20,
		Prefetch:          DefaultKafkaFetchPrefetch,
	}, resolved)

	partial := &KafkaFetch{Prefetch: 1}
	resolved, err = partial.Resolved()
	assert.NoError(t, err)
	assert.Equal(t, 100<<20, resolved.MaxBytes)
	assert.Equal(t, 10<<20, resolved.MaxPartitionBytes)
	assert.Equal(t, 1, resolved.Prefetch)
}

// A bound that is zero, negative, inverted, or past int32 is a config error
// that names its field, not a fetch that franz-go silently clamps.
func TestConfigTemplating_Load_KafkaFetchRejectsBadBounds(t *testing.T) {
	coverage.Covers(t, "config.templating")
	for _, tt := range []struct {
		name string
		in   KafkaFetch
		want string
	}{
		{"negative prefetch", KafkaFetch{Prefetch: -1}, "prefetch"},
		{"zero max_bytes", KafkaFetch{MaxBytes: -5}, "max_bytes"},
		{"negative partition bytes", KafkaFetch{MaxPartitionBytes: -1}, "max_partition_bytes"},
		{"partition above broker", KafkaFetch{MaxBytes: 1 << 20, MaxPartitionBytes: 2 << 20}, "max_partition_bytes"},
		{"max_bytes past int32", KafkaFetch{MaxBytes: 1 << 31}, "max_bytes"},
		{"partition bytes past int32", KafkaFetch{MaxBytes: 1 << 31, MaxPartitionBytes: 1 << 31}, "max_bytes"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.in.Resolved()
			assert.Error(t, err)
			assert.That(t, strings.Contains(err.Error(), tt.want))
		})
	}
}
```

Note on the table: a field left at zero is "absent" and takes its default, so the negative cases are what exercise "must be positive". The `max_bytes` case with `-5` is deliberately negative for that reason.

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cd .claude/worktrees/kafka-fetch-bound && go test ./internal/config/ -run 'KafkaFetch' -v`
Expected: compile failure, `undefined: KafkaFetch` and `conf.Pipeline.Source.Kafka.Fetch undefined`.

- [ ] **Step 3: Add the struct, the field, the constants, and `Resolved`**

In `internal/config/config.go`, replace the `KafkaSource` struct with:

```go
type KafkaSource struct {
	Brokers         []string `yaml:"brokers"`
	GroupID         string   `yaml:"group_id"`
	AutoOffsetReset string   `yaml:"auto_offset_reset" jsonschema:"enum=earliest,enum=latest"`
	Topics          []string `yaml:"topics"`

	SecurityProtocol string     `yaml:"security_protocol,omitempty" jsonschema:"enum=SASL_SSL,enum=SSL,enum=SASL_PLAINTEXT,enum=PLAINTEXT"`
	SSL              *KafkaSSL  `yaml:"ssl,omitempty"`
	SASL             *KafkaSASL `yaml:"sasl,omitempty"`

	// Bounds how far the consumer reads ahead of the pipeline. Omit the
	// block to accept the defaults, which bound a backlog replay to a few
	// fetches rather than the backlog.
	Fetch *KafkaFetch `yaml:"fetch,omitempty"`
}

// Defaults for KafkaFetch. The two byte values are what the source set
// before the block existed. The prefetch default is measured: the smallest
// depth within 5% of unbounded throughput on a 3M message backlog. See
// docs/superpowers/specs/2026-09-10-kafka-fetch-bound-design.md.
const (
	DefaultKafkaFetchMaxBytes          = 100 << 20
	DefaultKafkaFetchMaxPartitionBytes = 10 << 20
	DefaultKafkaFetchPrefetch          = 2
)

// KafkaFetch bounds the consumer's read-ahead. The source holds at most
// prefetch fetches in the channel the pipeline reads from, plus the one it is
// waiting to send, and franz-go holds one more per broker. In bytes of
// payload, the worst case is (prefetch + 2) x brokers x max_bytes and the
// typical case is (prefetch + 2) x partitions x max_partition_bytes.
type KafkaFetch struct {
	// Bytes one fetch may return per broker. Kafka's fetch.max.bytes.
	MaxBytes int `yaml:"max_bytes,omitempty" jsonschema:"minimum=1"`
	// Bytes one fetch may return per partition. Kafka's max.partition.fetch.bytes.
	MaxPartitionBytes int `yaml:"max_partition_bytes,omitempty" jsonschema:"minimum=1"`
	// Fetches held ahead of the pipeline.
	Prefetch int `yaml:"prefetch,omitempty" jsonschema:"minimum=1"`
}

// Resolved fills absent fields with their defaults and checks the bounds.
// A nil receiver is the absent block.
func (f *KafkaFetch) Resolved() (KafkaFetch, error) {
	out := KafkaFetch{
		MaxBytes:          DefaultKafkaFetchMaxBytes,
		MaxPartitionBytes: DefaultKafkaFetchMaxPartitionBytes,
		Prefetch:          DefaultKafkaFetchPrefetch,
	}
	if f == nil {
		return out, nil
	}
	if f.MaxBytes != 0 {
		out.MaxBytes = f.MaxBytes
	}
	if f.MaxPartitionBytes != 0 {
		out.MaxPartitionBytes = f.MaxPartitionBytes
	}
	if f.Prefetch != 0 {
		out.Prefetch = f.Prefetch
	}

	if out.MaxBytes < 1 || out.MaxBytes > math.MaxInt32 {
		return out, errs.New(errs.CodeSourceInvalid, "kafka source: fetch.max_bytes must be between 1 and %d, got %d", math.MaxInt32, out.MaxBytes)
	}
	if out.MaxPartitionBytes < 1 || out.MaxPartitionBytes > math.MaxInt32 {
		return out, errs.New(errs.CodeSourceInvalid, "kafka source: fetch.max_partition_bytes must be between 1 and %d, got %d", math.MaxInt32, out.MaxPartitionBytes)
	}
	if out.MaxPartitionBytes > out.MaxBytes {
		return out, errs.New(errs.CodeSourceInvalid, "kafka source: fetch.max_partition_bytes (%d) must not exceed fetch.max_bytes (%d)", out.MaxPartitionBytes, out.MaxBytes)
	}
	if out.Prefetch < 1 {
		return out, errs.New(errs.CodeSourceInvalid, "kafka source: fetch.prefetch must be at least 1, got %d", out.Prefetch)
	}
	return out, nil
}
```

Add `"math"` and `"github.com/turbolytics/sql-flow/internal/errs"` to the imports of `config.go` if they are not already there (`load.go` in the same package already imports `errs`; check `config.go` on its own).

- [ ] **Step 4: Run the tests and watch them pass**

Run: `go test ./internal/config/ -run 'KafkaFetch' -v`
Expected: PASS for all three, six subtests in the third.

- [ ] **Step 5: Regenerate the schema and confirm the golden test passes**

Run: `make schema && go test ./internal/schema/ ./internal/validate/ ./internal/config/`
Expected: `regenerated internal/validate/schemas/config.json`, then PASS. `git diff --stat internal/validate/schemas/config.json` shows the `fetch` object with three integer properties, each carrying `"minimum": 1`, under both the kafka source and nowhere else.

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/config/kafka_fetch_test.go internal/validate/schemas/config.json
git commit -m "config: a fetch block bounds the kafka source's read-ahead

max_bytes and max_partition_bytes are franz-go's two fetch sizes, at the
values the source already used. prefetch is the depth of the channel
between the poll goroutine and the pipeline, which was a fixed 100 slots,
one whole fetch per slot. The schema is regenerated from the tags."
```

---

### Task 2: Wire the block through, and prove the bound with a failing-first integration test

**Files:**
- Modify: `internal/sources/init.go:46-73` (the kafka builder)
- Modify: `internal/kafka/source.go:57-64` (`NewSource` default) and `:172-241` (poll loop comment)
- Test: `internal/kafka/source_test.go` (append one test and one helper)

**Interfaces:**
- Consumes: `config.KafkaFetch.Resolved()`, `config.DefaultKafkaFetchPrefetch` from Task 1; the existing `tkafka.WithChannelBuffer(int)`.
- Produces: nothing new. The behavior: a source built with `WithChannelBuffer(n)` never holds more than `n` fetches in its channel.

- [ ] **Step 1: Write the failing integration test**

Append to `internal/kafka/source_test.go`:

```go
// produceSized writes n records of size bytes, one ProduceSync per record so
// each is its own producer batch. A fetch then carries at most
// FetchMaxPartitionBytes / size records, and the count coming off the
// stream can be reasoned about in fetches.
func produceSized(t *testing.T, client *kgo.Client, topic string, n, size int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	value := []byte(strings.Repeat("x", size))
	for i := 0; i < n; i++ {
		res := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value})
		assert.NoError(t, res.FirstErr())
	}
}

// A backlog used to be held in memory in full: the poll goroutine handed
// every fetch to a channel with 100 slots, one whole fetch per slot, so one
// partition could hold 1 GiB of payload before the goroutine blocked. The
// channel depth is now the bound, and the default depth is small. With
// 64 KiB fetches, a reader that takes nothing must leave a source built
// with the defaults holding a few fetches, not the topic.
func TestIntegrationSourceKafka_ReadAheadIsBoundedByPrefetch(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	broker := brokerOrFail(t)
	topic := fmt.Sprintf("turbine-prefetch-%d", time.Now().UnixNano())

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	assert.NoError(t, err)
	defer producer.Close()
	// 2,000 records of 1 KiB: about 32 fetches at the sizes below.
	produceSized(t, producer, topic, 2000, 1024)

	client := newTestClient(t, broker, topic, topic,
		kgo.FetchMaxBytes(64<<10),
		kgo.FetchMaxPartitionBytes(64<<10),
	)
	// No WithChannelBuffer: this is the depth every pipeline gets.
	src, err := NewSource(client)
	assert.NoError(t, err)

	// Nobody reads. The poll goroutine fills the channel and blocks on the
	// next send; franz-go fills one more fetch and stops.
	stream := src.Stream()
	time.Sleep(2 * time.Second)

	// Close drops the fetch in the goroutine's hand and closes the channel.
	// What the channel held stays readable, and that is the read-ahead.
	assert.NoError(t, src.Close())
	drained := 0
	for batch := range stream {
		drained += len(batch)
	}

	// One fetch is about 64 records. One extra fetch of slack allows a
	// broker that packs a partial batch. At the old depth of 100 this
	// drained all 2,000.
	const recordsPerFetch = 64
	limit := (config.DefaultKafkaFetchPrefetch + 1) * recordsPerFetch
	assert.That(t, drained > 0)
	if drained > limit {
		t.Fatalf("read-ahead held %d records; the default depth of %d fetches allows about %d",
			drained, config.DefaultKafkaFetchPrefetch, limit)
	}
}
```

Add `"strings"` and `"github.com/turbolytics/sql-flow/internal/config"` to the test file's imports.

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/kafka/ -run TestIntegrationSourceKafka_ReadAheadIsBoundedByPrefetch -v -count=1`
Expected: FAIL with `read-ahead held 2000 records; the default depth of 2 fetches allows about 192`. The `NewSource` default is still 100, so every fetch of the 2 MB topic sits in the channel. If the count is a little under 2,000, that is the fetch in the goroutine's hand being dropped at close; the test still fails.

- [ ] **Step 3: Change the source default and wire the builder**

In `internal/kafka/source.go`, add the import `"github.com/turbolytics/sql-flow/internal/config"` and change the `NewSource` default:

```go
	s := &Source{
		client:        client,
		readTimeout:   5 * time.Second,
		channelBuffer: config.DefaultKafkaFetchPrefetch,

		logger: zap.NewNop(),
	}
```

Replace the `Stream` doc line and the poll loop's opening comment. Above `func (k *Source) Stream()`:

```go
// Stream hands each fetch to the pipeline through a channel channelBuffer
// deep. That depth is the read-ahead bound: a full channel blocks the poll
// goroutine, and franz-go stops fetching from a broker whose last fetch is
// unpolled, so back-pressure reaches the wire. The source holds at most
// channelBuffer fetches here, one in hand, and one per broker inside the
// client. Before this was a setting the depth was 100, which with 10 MiB
// fetches held a 10M message backlog in full.
func (k *Source) Stream() <-chan []core.Message {
```

In `internal/sources/init.go`, replace the two fetch options and the source construction:

```go
		fetch, err := c.Kafka.Fetch.Resolved()
		if err != nil {
			return nil, err
		}

		opts := []kgo.Opt{
			kgo.SeedBrokers(brokers...),
			kgo.ConsumerGroup(c.Kafka.GroupID),
			kgo.ConsumeTopics(c.Kafka.Topics...),
			kgo.ConsumeResetOffset(resetOffset),
			kgo.DisableAutoCommit(),
			kgo.AdjustFetchOffsetsFn(seeker.Adjust),
			kgo.FetchMaxPartitionBytes(int32(fetch.MaxPartitionBytes)),
			kgo.FetchMaxBytes(int32(fetch.MaxBytes)),
		}
```

and

```go
		k, err := tkafka.NewSource(client,
			tkafka.WithLogger(l),
			tkafka.WithSeeker(seeker),
			tkafka.WithChannelBuffer(fetch.Prefetch),
		)
		return k, err
```

Add the resolved values to the builder's startup log line so an operator can read the bound from the log:

```go
		l.Info(
			"initializing kafka source",
			zap.String("topics", fmt.Sprintf("%v", c.Kafka.Topics)),
			zap.String("group.id", c.Kafka.GroupID),
			zap.String("auto.offset.reset", c.Kafka.AutoOffsetReset),
		)
```

becomes, after `fetch` is resolved (move the log below the `Resolved` call):

```go
		l.Info(
			"initializing kafka source",
			zap.String("topics", fmt.Sprintf("%v", c.Kafka.Topics)),
			zap.String("group.id", c.Kafka.GroupID),
			zap.String("auto.offset.reset", c.Kafka.AutoOffsetReset),
			zap.Int("fetch.max_bytes", fetch.MaxBytes),
			zap.Int("fetch.max_partition_bytes", fetch.MaxPartitionBytes),
			zap.Int("fetch.prefetch", fetch.Prefetch),
		)
```

- [ ] **Step 4: Run the package tests and the unit pass**

Run: `go test ./internal/kafka/ -run 'TestIntegrationSourceKafka' -v -count=1`
Expected: all integration tests PASS, including `ReadAheadIsBoundedByPrefetch` with a drained count at or under 192.

Run: `make test-go`
Expected: build, vet, gofmt clean, and the `-short -race` pass green. `go build ./...` confirms no import cycle from `kafka` importing `config`.

- [ ] **Step 5: Confirm a shipped config still loads and validates**

Run: `go run ./cmd/sqlflow config validate dev/config/examples/benchmark.structured.mem.yml`
Expected: `valid`. The example has no `fetch` block and takes the defaults.

- [ ] **Step 6: Commit**

```bash
git add internal/kafka/source.go internal/kafka/source_test.go internal/sources/init.go
git commit -m "kafka source: the channel depth is the read-ahead bound, default 2

The poll goroutine handed every fetch to a 100 slot channel, one whole
fetch per slot: 1 GiB of payload per partition before anything blocked,
which is why a 10M message backlog sat fully in memory (#162). The depth
now comes from fetch.prefetch, the fetch sizes from the same block, and
the startup log prints all three.

The integration test builds a source with the defaults, fills it with
64 KiB fetches, reads nothing, then drains it: a few fetches come out,
not the topic. At depth 100 it drained all 2,000 records."
```

---

### Task 3: Measure the default on the 3M backlog cell

**Files:**
- Create (untracked, deleted at the end): `dev/config/examples/benchmark.prefetch.yml` in the worktree
- Create: `<scratchpad>/prefetch/run.sh` and `<scratchpad>/prefetch/uv-python`
- Modify: `internal/config/config.go` (`DefaultKafkaFetchPrefetch`) only if the measurement says so

**Interfaces:**
- Consumes: the benchmark harness `scripts/benchmark-container.sh <msgs> <batch> <config>`, which reads `SQLFLOW_PYTHON` and prints `total_throughput_per_second`, `peak_memory_bytes`, `peak_anon_bytes`.
- Produces: the table for the PR description and the final value of `DefaultKafkaFetchPrefetch`.

- [ ] **Step 1: Pre-flight the box**

Run:

```bash
docker ps --format '{{.Names}}' | grep -q '^kafka1$' && echo kafka up
docker run --rm alpine sh -c 'df -h / | tail -1'
uptime
```

Expected: Kafka up, at least 6 GB free on the VM (a 3M topic is about 600 MB and the harness creates one per run), and the load average noted for the table.

- [ ] **Step 2: Write the benchmark config with a templated prefetch**

Create `dev/config/examples/benchmark.prefetch.yml` in the worktree. It is `benchmark.structured.mem.yml` with one addition:

```yaml
commands:
  - name: create source buffer table
    sql: |
      CREATE TABLE source (
            event STRING,
            properties STRUCT(city TEXT),
      );

pipeline:
  batch_size: {{ SQLFLOW_BATCH_SIZE|default(500) }}

  source:
    type: kafka
    kafka:
      brokers: [{{ SQLFLOW_KAFKA_BROKERS|default('localhost:9092') }}]
      group_id: {{ SQLFLOW_GROUP_ID|default('benchmark') }}
      auto_offset_reset: earliest
      topics:
        - "{{ SQLFLOW_TOPIC|default('benchmark-input') }}"
      fetch:
        prefetch: {{ SQLFLOW_PREFETCH|default(2) }}

  handler:
    type: "handlers.StructuredBatch"
    table: source
    sql: |
      SELECT
        properties.city as city,
        COUNT(*) as count
      FROM source
      GROUP BY properties.city;

  sink:
    type: noop
```

Do not commit this file. It exists so one config can sweep the value.

- [ ] **Step 3: Write the runner**

Create `<scratchpad>/prefetch/uv-python`:

```bash
#!/usr/bin/env bash
exec uv run --project /Users/danielmican/code/github.com/turbolytics/sql-flow/.claude/worktrees/kafka-fetch-bound python "$@"
```

Create `<scratchpad>/prefetch/run.sh`:

```bash
#!/usr/bin/env bash
# Sweeps fetch.prefetch on the 3M backlog cell. One topic per run, deleted
# afterwards so the VM disk does not fill. Three runs per value.
set -euo pipefail
W=/Users/danielmican/code/github.com/turbolytics/sql-flow/.claude/worktrees/kafka-fetch-bound
OUT="$(dirname "$0")"
export SQLFLOW_PYTHON="$OUT/uv-python"
cd "$W"
for prefetch in 100 1 2 4 8; do
  for run in 1 2 3; do
    topic="bench-prefetch-$prefetch-$run-$(date +%s)"
    log="$OUT/prefetch-$prefetch-run$run.log"
    load=$(uptime | sed 's/.*load averages*: //')
    SQLFLOW_PREFETCH=$prefetch BENCH_TOPIC=$topic \
      ./scripts/benchmark-container.sh 3000000 5000 dev/config/examples/benchmark.prefetch.yml > "$log" 2>&1 || true
    tp=$(grep -o 'total_throughput_per_second[^,}]*' "$log" | tail -1 | grep -o '[0-9.]*$')
    peak=$(grep -o 'peak_memory_bytes: [0-9]*' "$log" | tail -1 | grep -o '[0-9]*$')
    anon=$(grep -o 'peak_anon_bytes: [0-9]*' "$log" | tail -1 | grep -o '[0-9]*$')
    echo "prefetch=$prefetch run=$run throughput=${tp:-fail} peak=$((${peak:-0}/1048576))MiB anon=$((${anon:-0}/1048576))MiB load=$load" | tee -a "$OUT/summary.txt"
    docker exec kafka1 kafka-topics --bootstrap-server localhost:19092 --delete --topic "$topic" >/dev/null 2>&1 || true
  done
done
```

`chmod +x` both. The `100` value is the unbounded baseline on the same build, so the comparison is not against yesterday's number.

- [ ] **Step 4: Run it in the background and wait**

Run: `nohup <scratchpad>/prefetch/run.sh > <scratchpad>/prefetch/run.out 2>&1 &`
Expected: fifteen lines in `summary.txt` after roughly 40 minutes. Do not run anything else heavy meanwhile; the load column is the evidence that the box was comparable across values.

Check on it with: `cat <scratchpad>/prefetch/summary.txt`.

- [ ] **Step 5: Pick the default**

Compute the median throughput and median peak per value. The rule from the spec: the smallest `prefetch` whose median throughput is within 5% of the `prefetch=100` median. If none qualifies, the smallest within 10%, and the PR says so.

If the winner is not 2, change `DefaultKafkaFetchPrefetch` in `internal/config/config.go`, update the `default(2)` in the untracked benchmark config, and re-run `go test ./internal/config/ ./internal/kafka/ -count=1` (the config test reads the constant, so it does not need editing).

- [ ] **Step 6: Record the table and commit**

Write the five-row table (value, median throughput, delta from baseline, median container peak, median working set, load) into `docs/superpowers/specs/2026-09-10-kafka-fetch-bound-design.md` under "Choosing the default", replacing the sentence that begins "The PR description carries the table" with the table itself plus one sentence naming the chosen default. Then:

```bash
rm dev/config/examples/benchmark.prefetch.yml
git add docs/superpowers/specs/2026-09-10-kafka-fetch-bound-design.md internal/config/config.go
git commit -m "kafka source: prefetch default measured on the 3M backlog cell

<one line: chosen value, its throughput against unbounded, its peak against 1,040 MiB>"
```

If the constant did not change, leave `internal/config/config.go` out of the `git add`.

---

### Task 4: Docs on turbolytics.io

**Files:**
- Modify: `/Users/danielmican/code/github.com/turbolytics/turbolytics.io/src/content/docs/sqlflow/introduction/configuration.md:162` (after "Each source's configuration lives under its own key.")

**Interfaces:**
- Consumes: the final default from Task 3.
- Produces: the published reference for the block.

- [ ] **Step 1: Add the block to the Source Configuration section**

In the turbolytics.io checkout, on branch `docs/benchmarks-2026-09-09`, after the line `Each source's configuration lives under its own key. For \`type: kafka\`, that's the \`kafka\` key.` add:

````markdown

#### Bounding the Kafka source's read-ahead

The Kafka source fetches ahead of the pipeline. `fetch` bounds how far. Every field is optional.

```yaml
pipeline:
  ...
  source:
    type: kafka
    kafka:
      brokers: [{{ SQLFLOW_KAFKA_BROKERS|default('localhost:9092') }}]
      group_id: test
      auto_offset_reset: earliest
      topics:
        - "input-simple-agg-mem"
      fetch:
        max_bytes: 104857600
        max_partition_bytes: 10485760
        prefetch: 2
```

| Field | Default | Meaning |
| --- | --- | --- |
| `max_bytes` | 104857600 (100 MiB) | Bytes one fetch may return per broker. |
| `max_partition_bytes` | 10485760 (10 MiB) | Bytes one fetch may return per partition. |
| `prefetch` | 2 | Fetches held ahead of the pipeline. |

The source holds at most `prefetch` fetches ahead of the pipeline, plus one it is waiting to hand over, and the client holds one more per broker. The payload in memory during a backlog replay is therefore at most `(prefetch + 2) x brokers x max_bytes`, and typically `(prefetch + 2) x partitions x max_partition_bytes`. Resident memory runs about three times the payload for small JSON messages.

Raise `prefetch` on a fast pipeline that stalls waiting for the broker. Lower `max_partition_bytes` on a topic of small messages when the container's memory limit is tight. The [benchmarks page](/products/sqlflow/benchmarks) measures what the default costs.
````

Replace `2` with the measured default from Task 3 in all three places if it changed.

- [ ] **Step 2: Build and check**

Run, in the turbolytics.io checkout: `npm run build 2>&1 | tail -3 && grep -c "—" src/content/docs/sqlflow/introduction/configuration.md`
Expected: build succeeds, em dash count `0`.

- [ ] **Step 3: Commit on the docs branch**

```bash
git add src/content/docs/sqlflow/introduction/configuration.md
git commit -m "docs: the kafka source's fetch block, and the bound it gives"
```

Push is held for the user, as with the rest of that branch.

---

### Task 5: Open the PR, get CI green, regenerate the coverage matrix

**Files:**
- Modify: `docs/coverage/matrix.json`, `docs/coverage/matrix.md` (from CI artifacts only)

- [ ] **Step 1: Push and open the PR**

```bash
git push -u origin feat/kafka-fetch-bound
gh pr create --title "Kafka source: bound the read-ahead with a fetch block" --body-file - <<'EOF'
## Why

A backlog replay held the backlog in memory: 1,040 MiB at 3M messages, 2,720 MiB at 10M, about 300 bytes per waiting message, released seconds after catch-up. Not a leak. franz-go bounds itself to one fetch per broker, but the poll goroutine handed every fetch to a channel with 100 slots, one whole fetch per slot: 1 GiB of payload per partition before anything blocked. Tracks the memory half of #162.

## What

A `fetch` block on the kafka source:

- `max_bytes`, per broker, default 100 MiB. Unchanged from the value the source already set.
- `max_partition_bytes`, per partition, default 10 MiB. Unchanged.
- `prefetch`, fetches held ahead of the pipeline, default <N>. Was a fixed 100.

Worst case payload in memory is `(prefetch + 2) x brokers x max_bytes`. The startup log prints all three. The schema is regenerated from the tags.

## Default

Measured on the 3M backlog cell, StructuredBatch at batch 5000, three runs per value, medians:

<table from Task 3>

## Tests

- `TestIntegrationSourceKafka_ReadAheadIsBoundedByPrefetch`: a source built with the defaults, 64 KiB fetches, nobody reading, drains a few fetches after close. Drained all 2,000 records at depth 100.
- Config: the block loads, absent fields default, each bound rejects its input naming the field.
- Schema golden regenerated.

Spec: `docs/superpowers/specs/2026-09-10-kafka-fetch-bound-design.md`.
EOF
```

Fill `<N>` and the table from Task 3 before running.

- [ ] **Step 2: Watch CI**

Run: `gh pr checks --watch`
Expected: unit, integration, release, and coverage jobs green. If the coverage gate fails, the cause is a test missing its `coverage.Covers` first line or the matrix being stale; fix the test, never regenerate the matrix locally.

- [ ] **Step 3: Regenerate the coverage matrix from the CI artifacts**

```bash
run=$(gh run list --branch feat/kafka-fetch-bound --limit 1 --json databaseId --jq '.[0].databaseId')
rm -rf /tmp/cov && mkdir -p /tmp/cov
gh run download "$run" -n report-unit -D /tmp/cov/unit
gh run download "$run" -n report-integration -D /tmp/cov/integration
gh run download "$run" -n report-release -D /tmp/cov/release
make coverage-write
git add docs/coverage/matrix.json docs/coverage/matrix.md
git commit -m "coverage: matrix from the fetch-bound CI run"
git push
```

Expected: `source.kafka` still shows unit, integration, and release covered; the diff adds the new test name to its cell.

- [ ] **Step 4: Merge when green and the user says so**

The user merges. Do not merge on their behalf.

---

### Task 6: Acceptance, the 100M backlog run

**Files:**
- Create: `<scratchpad>/bench-100m/run.sh`
- Modify (turbolytics.io, branch `docs/benchmarks-2026-09-09`): `src/data/benchmark.ts` (`backlog` array and its comment), `src/pages/products/sqlflow/benchmarks.astro` (the "Memory under load" section's footnote)

**Interfaces:**
- Consumes: merged main with the default from Task 3, and `scripts/benchmark-container.sh`.
- Produces: one row per handler at 100M, container peak and working set beside throughput.

- [ ] **Step 1: Make room on the Docker VM**

The topic needs about 19 GB and the VM has 15.6 GB free. Look before deleting:

```bash
docker images --format '{{.Repository}}:{{.Tag}} {{.Size}} {{.CreatedSince}}' | grep -v 'sql-flow:main\|sql-flow:latest\|cp-kafka\|zookeeper\|golang\|debian\|curlimages'
docker volume ls --filter dangling=true
```

Delete only what that shows: the dated `turbolytics/sql-flow:v1.0.x-*` build tags and the Python-era images, and dangling volumes. Not the `planningops-*` volumes, not the Kafka data volume, not the `clickhouse` volumes.

```bash
docker images --format '{{.Repository}}:{{.Tag}}' | grep 'sql-flow:v1.0.[0-9]-[0-9]*-g\|sql-flow:.*-dirty\|sql-flow:fix-\|sql-flow:memleak' | xargs docker rmi
docker volume prune -f
docker run --rm alpine sh -c 'df -h / | tail -1'
```

Expected: at least 22 GB free. If it is not, raise the VM disk in Docker Desktop settings rather than deleting more, and say so.

- [ ] **Step 2: Pull main and build the image the harness uses**

```bash
cd /Users/danielmican/code/github.com/turbolytics/sql-flow && git pull --ff-only origin main
```

The harness builds `bin/sqlflow-linux` from the checkout on each run, so this is enough.

- [ ] **Step 3: Write the runner**

Create `<scratchpad>/bench-100m/run.sh`:

```bash
#!/usr/bin/env bash
# The acceptance run: a 100M message backlog, default fetch bound, both
# handlers. One run each; the topic is deleted between them.
set -euo pipefail
R=/Users/danielmican/code/github.com/turbolytics/sql-flow
OUT="$(dirname "$0")"
export SQLFLOW_PYTHON="$OUT/uv-python"
cd "$R"
for cfg in benchmark.structured.mem.yml benchmark.inferred.mem.yml; do
  topic="bench-100m-${cfg%%.*}-$(date +%s)"
  log="$OUT/$cfg.log"
  load=$(uptime | sed 's/.*load averages*: //')
  BENCH_TOPIC=$topic ./scripts/benchmark-container.sh 100000000 5000 "dev/config/examples/$cfg" > "$log" 2>&1 || true
  tp=$(grep -o 'total_throughput_per_second[^,}]*' "$log" | tail -1 | grep -o '[0-9.]*$')
  peak=$(grep -o 'peak_memory_bytes: [0-9]*' "$log" | tail -1 | grep -o '[0-9]*$')
  anon=$(grep -o 'peak_anon_bytes: [0-9]*' "$log" | tail -1 | grep -o '[0-9]*$')
  echo "config=$cfg msgs=100000000 throughput=${tp:-fail} peak=$((${peak:-0}/1048576))MiB anon=$((${anon:-0}/1048576))MiB load=$load" | tee -a "$OUT/summary.txt"
  docker exec kafka1 kafka-topics --bootstrap-server localhost:19092 --delete --topic "$topic" >/dev/null 2>&1 || true
done
```

Copy `uv-python` from Task 3 beside it with the path changed to `$R`. `chmod +x` both.

- [ ] **Step 4: Run it in the background**

Run: `nohup <scratchpad>/bench-100m/run.sh > <scratchpad>/bench-100m/run.out 2>&1 &`
Expected: publishing 100M messages with the Python publisher is the long part, on the order of an hour per topic; the consume is under two minutes for StructuredBatch and about seven for InferredMemBatch. Two lines in `summary.txt` when done. Check the VM disk once during the publish with the `df` command above.

- [ ] **Step 5: Put the numbers on the site**

In `src/data/benchmark.ts`, append the two 100M rows to `backlog` and rewrite its comment so it says the consumer's read-ahead is bounded by `fetch.prefetch` as of the merged PR, that the 3M and 10M rows predate the bound, and what the 100M peak was with the bound in place. In `benchmarks.astro`, the "Memory under load" footnote that cites #162 says the bound landed and links the PR. Build, test, em dash check, commit on `docs/benchmarks-2026-09-09`. Push held.

- [ ] **Step 6: Report on #162**

Post a comment on sql-flow #162 with the 100M line: throughput, container peak, working set, the default in force, and the sentence "peak memory is now a setting". Leave the issue open: `max_memory` and `max_state_size` are its other half.

---

## Self-review

**Spec coverage.** Config block, defaults, validation rules, int32 check: Task 1. Schema regeneration: Task 1 step 5. Engine wiring, `NewSource` default, poll loop comment, `MaxConcurrentFetches` left alone: Task 2. Failing-first integration test with the spec's numbers: Task 2. Default measured at 1, 2, 4, 8 against a same-build baseline, 5% rule with the 10% fallback: Task 3. Site docs with the bound formula: Task 4. Acceptance 100M run and disk clearing: Task 6. Coverage matrix from CI: Task 5. Out of scope items have no task, as intended.

**Placeholders.** `<N>` and `<table from Task 3>` in the PR body are filled from Task 3's output before the command runs, and the step says so. `<scratchpad>` is the session scratchpad directory named in the environment. No other placeholders.

**Type consistency.** `KafkaFetch{MaxBytes, MaxPartitionBytes, Prefetch int}` and `Resolved() (KafkaFetch, error)` are used identically in Tasks 1 and 2. `DefaultKafkaFetchPrefetch` is read by the config test, the kafka source, and Task 3. `WithChannelBuffer(int)` already exists with that signature.

**One correction made during review.** The spec places validation "in internal/config" as "a load error". `Load` only decodes; the existing pattern reports invalid source settings from the builder with `errs.CodeSourceInvalid`. The plan keeps the rule and the tests in `internal/config` through `Resolved`, and the error surfaces where the source is built, at startup before any message is consumed. That is the same moment an operator sees it.
