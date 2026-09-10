# Kafka source read-ahead bound

Status: design, 2026-09-10. Tracks sql-flow #162, the memory half.

## The problem

A pipeline draining a backlog holds the backlog in memory. Measured on main at
`46a1bd1`, StructuredBatch at batch 5000, noop sink: a 3M message topic peaks
at 1,040 MiB and a 10M topic at 2,720 MiB, about 300 bytes per waiting
message. The memory is Go heap and it is released within seconds of catching
up. It is not a leak. It is read-ahead with no bound.

franz-go bounds itself. `internal/sources/init.go` sets `FetchMaxBytes` to
100 MiB per broker and `FetchMaxPartitionBytes` to 10 MiB per partition, and
the client holds one fetch per broker until it is polled.

sqlflow removes the bound. The poll goroutine in `internal/kafka/source.go`
polls without pause and hands every fetch to `streamChan`, a channel with 100
slots. One slot is a whole fetch. With one partition the channel holds up to
100 x 10 MiB of payload before the goroutine blocks: 1 GiB, or ten million
100 byte messages. That is why a 10M backlog sat fully in memory, and why a
100M backlog would grow to the channel's limit rather than to the engine's.

The consume loop in `internal/core/turbine.go` reads one fetch at a time and
re-batches it to `batch_size`. Nothing tells the source how far ahead it may
run.

## The change

The channel depth becomes a setting, with a small default, beside the two
fetch sizes it multiplies. Bounded by default: a pipeline gets bounded memory
on a backlog without a config change.

### Config

A new optional block on the kafka source:

```yaml
source:
  type: kafka
  kafka:
    brokers: [...]
    group_id: ...
    auto_offset_reset: earliest
    topics: [...]
    fetch:
      max_bytes: 104857600          # per broker, per fetch. Default 100 MiB.
      max_partition_bytes: 10485760 # per partition, per fetch. Default 10 MiB.
      prefetch: 2                   # fetches held ahead of the pipeline.
```

`max_bytes` and `max_partition_bytes` map one to one onto franz-go's
`FetchMaxBytes` and `FetchMaxPartitionBytes`, and their defaults are the values
`init.go` sets today. `prefetch` is the channel depth. Its default is measured,
not guessed; see "Choosing the default".

The bound an operator can compute from the config, in bytes of payload:

- Worst case: `(prefetch + 2) x brokers x max_bytes`. The channel holds
  `prefetch` fetches, the poll goroutine holds one it is waiting to send, and
  franz-go holds one per broker.
- Typical: `(prefetch + 2) x partitions x max_partition_bytes`, since a fetch
  from a broker carries at most `max_partition_bytes` per partition.

Resident memory runs about three times the payload for small JSON messages.
The `core.Message` struct is 72 bytes, the fetch buffer stays alive while any
value slices into it, and the Go heap carries its usual slack.

Validation, in `internal/config`:

- Every field is optional. An absent block means all three defaults.
- A present field must be positive.
- `max_partition_bytes` must not exceed `max_bytes`.
- Both byte fields must fit an int32, because franz-go takes int32.
- A violation is a load error naming the field, in the style of the existing
  loader errors.

The JSON schema golden at `internal/validate/schemas/config.json` is an
artifact of the struct tags. `make schema` regenerates it, and
`TestConfigSchema_CommittedFileMatchesTheTypes` fails until it is regenerated.

### Engine

`internal/sources/init.go` reads the block, applies the defaults, passes the
two byte values to the `kgo` options it already sets, and passes `prefetch` to
`tkafka.WithChannelBuffer`. That option already exists; nothing calls it.

`internal/kafka/source.go` needs no logic change. A full channel already blocks
the poll goroutine on send, and franz-go stops fetching from a broker whose
last fetch is unpolled, so a full channel back-pressures to the wire. The
`NewSource` default of 100 changes to the new default, so a source built
without the option is bounded too. The poll loop comment states the bound.

`kgo.MaxConcurrentFetches` is left at its default. The per-broker rule
already bounds fetches in flight to the broker count, and the setting exists
for many-broker clusters, which the benchmark cannot measure.

No new goroutines. No byte counting in sqlflow. The pipeline's batch loop is
untouched.

### Choosing the default

The default `prefetch` is the smallest value that keeps backlog throughput
within 5% of today's. Measured before the PR opens, on the same harness and
box as the published figures:

- Cell: 3M message backlog, StructuredBatch, batch 5000, noop sink,
  `scripts/benchmark-container.sh 3000000 5000`.
- Values: `prefetch` 1, 2, 4, 8, against the unbounded baseline of 100.
- Three runs each, medians, host load stated.
- Baseline on main `46a1bd1`: 1,075,346 msgs/sec, 1,040 MiB container peak.

The PR description carries the table. The expectation is that `prefetch` 2
holds throughput and peaks in the low hundreds of MiB, because the pipeline
consumes one fetch while the next two wait and franz-go fills a third.

If no value within 5% exists, the default is the smallest value within 10%
and the PR says so. Bounded memory is the requirement; the 5% is a target.

### Tests

One failing-first integration test in `internal/kafka`, first line
`coverage.Covers(t, "source.kafka")`, named
`TestIntegrationSourceKafka_ReadAheadIsBoundedByPrefetch`:

1. Build a client with `FetchMaxPartitionBytes(64 KiB)` and a source with
   `WithChannelBuffer(1)`.
2. Produce 2,000 records of 1 KiB each with `ProduceSync`, one per call, so
   each is its own producer batch and a fetch cannot carry more than about 64
   of them.
3. Call `Stream()` and read nothing for two seconds.
4. Close the source. The poll goroutine returns, drops the fetch in its hand,
   and closes the channel. Whatever the channel holds stays readable.
5. Drain the channel and count records. Assert the count is at most 128, two
   fetches, which allows one fetch of slack for a broker that packs a partial
   extra batch.

On today's code the poll goroutine keeps polling into 100 free slots, and the
drain returns all 2,000. That is the failing run. The test does not measure
bytes; the bound in bytes follows from the bound in fetches.

Config tests in `internal/config`, marked `config.templating` like their
neighbours: the block loads with all three fields, an absent block yields the
defaults, and each validation rule rejects its input with an error naming the
field.

The schema test in `internal/schema` covers the golden.

### Acceptance

The change is accepted by the run that motivated it: a 100M message backlog
on `scripts/benchmark-container.sh`, StructuredBatch at batch 5000, default
`prefetch`, container peak and working set reported beside throughput. Peak
memory is then a setting, not a property of the outage.

The run needs about 19 GB of topic on a Docker VM with 15.6 GB free. Before
it: delete the Python-era images and the unused volumes, or raise the VM disk.
The run is the last step of the plan, not a condition of the PR.

### Docs

The site's configuration page, `introduction/configuration.md` under Source
Configuration, gains the `fetch` block with the bound formula and the
defaults. The sql-flow example configs are unchanged: the default bounds them.
The turbolytics.io benchmarks page gains the 100M row and the note that the
backlog tier is now bounded, in the branch already open for the 2026-09-09
figures.

## Out of scope

- `max_memory` and `max_state_size` on the pipeline definition, the other
  half of #162.
- DuckDB's memory limit and the Arrow buffers in the handlers.
- Any change to the consume loop or to `batch_size` semantics.
- Websocket and webhook sources, which have their own channel buffers and no
  broker to back-pressure.
