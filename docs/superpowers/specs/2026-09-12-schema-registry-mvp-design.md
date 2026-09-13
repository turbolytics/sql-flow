# Schema registry MVP: Avro and JSON Schema through Kafka, both directions

Issue #272. Verified against `main` at ae8fd28 on 2026-09-12.

## The problem

Every byte the engine reads is assumed to be JSON, and every byte the Kafka
sink writes is JSON. A customer's topics carry Confluent-framed Avro. Nothing
in the config can say so, and nothing in the engine can decode it.

## Scope

In:

- Kafka source: `json` (today), `json_schema`, `avro`. The last two are framed
  by a Confluent-compatible schema registry.
- Kafka sink: the same three, with the sink registering or looking up its
  output schema.
- The dev stack gains a schema registry.
- One example config per direction, a framed-record producer, README docs.
- The memory soak runs once per format, raw JSON included.
- Go benchmarks per format, an A/B against `main` with `benchstat`, and a
  container throughput run per format.

Out, each a follow-up issue:

- Protobuf. Decoding needs `protocompile` plus `dynamicpb`. Encoding needs a
  descriptor synthesized from an Arrow schema. Neither belongs in an MVP.
- Message keys. `core.Message` has no key field today.
- `InferredDiskBatch` and `StructuredBatch` with a registry format. The disk
  path stages JSON files; a typed path would stage Parquet, which is its own
  change. `StructuredBatch` derives its schema from a DuckDB table, and a
  registry format has its own schema. Both combinations fail at start with
  `user.config.invalid`.
- More than one topic on a registry-backed source. One reader schema per
  pipeline, so one subject.
- `validate` fetching the registry schema and binding the SQL against it.
  The config surface below is shaped so this drops in.
- AWS Glue Schema Registry. Different header, different client.
- Subject naming strategies other than `<topic>-value`.

## Design

Three axes, each with one config key. The format decides bytes to Arrow and
back. The handler decides staging. The registry is shared.

### Config

```yaml
pipeline:
  schema_registry:                       # new, optional block
    url: http://localhost:8081
    auth:                                # optional
      username: '{{ SQLFLOW_SR_USER }}'
      password: '{{ SQLFLOW_SR_PASS }}'
      # or: bearer_token: '{{ SQLFLOW_SR_TOKEN }}'
    ssl:                                 # optional, same shape as kafka.ssl
      ca_location: /etc/certs/ca.pem

  source:
    type: kafka
    kafka:
      topics: [orders]
      value:                             # new, optional block
        format: avro                     # json | json_schema | avro; json is the default

  handler:
    type: handlers.InferredMemBatch      # unchanged
    sql: SELECT ...

  sink:
    type: kafka
    kafka:
      topic: orders-enriched
      value:                             # new, optional block
        format: avro                     # json | json_schema | avro; json is the default
        subject: orders-enriched-value   # default <topic>-value
        auto_register: true              # default true
```

Why `value` and not `format`: `sink.format.type: parquet` already exists,
parsed and ignored since the Python engine. Reusing the key would change its
meaning. `value` also names what it is, the record's value as opposed to its
key, which is where keys go when they arrive.

Why `schema_registry` is on the pipeline: a source and a sink almost always
share one registry, and a shared block is one URL and one credential. A
per-side override is not in the MVP.

Config rules, checked when the pipeline is built, each failing with
`user.config.invalid`:

- `format` other than `json` requires `pipeline.schema_registry`.
- A registry-backed source requires exactly one topic and a handler of type
  `handlers.InferredMemBatch`.
- `subject` and `auto_register` are sink-only keys.
- `auth` carries either `username` and `password` or `bearer_token`, not both.

The three struct changes:

```go
// Pipeline gains
SchemaRegistry *SchemaRegistry `yaml:"schema_registry,omitempty"`

type SchemaRegistry struct {
	URL  string              `yaml:"url"`
	Auth *SchemaRegistryAuth `yaml:"auth,omitempty"`
	SSL  *KafkaSSL           `yaml:"ssl,omitempty"`
}

type SchemaRegistryAuth struct {
	Username    string `yaml:"username,omitempty"`
	Password    string `yaml:"password,omitempty"`
	BearerToken string `yaml:"bearer_token,omitempty"`
}

// KafkaSource and KafkaSink both gain
Value *KafkaValue `yaml:"value,omitempty"`

type KafkaValue struct {
	Format       string `yaml:"format,omitempty" jsonschema:"enum=json,enum=json_schema,enum=avro"`
	Subject      string `yaml:"subject,omitempty"`
	AutoRegister *bool  `yaml:"auto_register,omitempty"`
}
```

`AutoRegister` is a pointer so an absent key means true. `make schema`
regenerates the JSON Schema from these tags.

### The `serde` package

A new package, `internal/serde`, owns everything between raw bytes and Arrow
that is not JSON inference:

```go
// Registry wraps the franz-go client with the caches the client itself does
// not have. Schemas by ID are immutable and cached for the process lifetime.
type Registry struct { ... }
func NewRegistry(conf config.SchemaRegistry) (*Registry, error)
func (r *Registry) Probe(ctx context.Context) error
func (r *Registry) SchemaByID(ctx context.Context, id int) (sr.Schema, error)
func (r *Registry) Latest(ctx context.Context, subject string) (sr.SubjectSchema, error)
func (r *Registry) Register(ctx context.Context, subject string, s sr.Schema) (int, error)
func (r *Registry) Lookup(ctx context.Context, subject string, s sr.Schema) (int, error)

// Decoder turns one framed record into one row of the reader schema.
type Decoder interface {
	Schema() *arrow.Schema
	Append(b *array.RecordBuilder, value []byte) error
}
func NewDecoder(ctx context.Context, format string, topic string, reg *Registry) (Decoder, error)

// Encoder turns a batch into framed records, one per row.
type Encoder interface {
	Encode(ctx context.Context, rec arrow.Record) ([][]byte, error)
}
func NewEncoder(format string, subject string, autoRegister bool, reg *Registry) (Encoder, error)
```

The franz-go module `github.com/twmb/franz-go/pkg/sr` v1.8.0 supplies the
client and `ConfluentHeader`, whose `DecodeID` and `AppendEncode` are the
header. `github.com/hamba/avro/v2` v2.31.0 supplies Avro. Both are new
dependencies. arrow-go's `arrow/avro` package was considered and not used: it
maps Avro enums to Arrow dictionaries, which a SQL user reads as a string, and
its type mapping panics into a recovered generic error where this engine wants
a coded one. The mapping below is about a hundred lines and is ours.

### Decoding

The reader schema is the subject's latest version at pipeline start, where
the subject is `<topic>-value`. The decoder converts it to an Arrow schema
once. That schema is the `batch` table for the life of the run, which is what
the handler SQL binds against and what `validate` will fetch later.

Each record:

1. `ConfluentHeader.DecodeID` strips the header. A record without the magic
   byte is `user.data.malformed`.
2. The writer schema is fetched by ID from the registry, through the cache.
   An ID the registry does not know is `user.data.schema_unknown`. A registry
   that does not answer is `system.source.unreachable`, and the fetch is
   retried three times over about a second before it is reported, because the
   alternative is a good record in the DLQ.
3. Avro: hamba's `SchemaCompatibility.Resolve(reader, writer)` produces the
   resolved schema, cached per writer ID, and `avro.Unmarshal` decodes the
   payload into a `map[string]any` shaped like the reader. This is Avro schema
   resolution as the specification defines it: added fields take their
   defaults, removed fields are dropped, and a change the reader cannot
   resolve is `user.data.malformed` naming both schemas. A payload that does
   not decode against its own writer schema is `user.data.malformed`.
4. JSON Schema: the payload after the header is JSON. It is extracted against
   the Arrow schema with the `appendJSONValue` machinery `StructuredBatch`
   already has. No inference runs. The schema is not validated against the
   payload in the MVP; a field the schema declares and the payload lacks is
   null.
5. The row is appended to the record builder. Kafka metadata columns are
   appended by the handler, as today.

Avro to Arrow:

| Avro | Arrow |
| --- | --- |
| `null` | `null` |
| `boolean` | `bool` |
| `int` | `int32` |
| `long` | `int64` |
| `float` | `float32` |
| `double` | `float64` |
| `bytes` | `binary` |
| `string`, `enum`, `uuid` | `utf8` |
| `fixed` | `binary` |
| `record` | `struct` |
| `array` | `list` |
| `map` | `map<utf8, T>` |
| `["null", T]` in either order | nullable `T` |
| `date` | `date32` |
| `time-millis`, `time-micros` | `time32[ms]`, `time64[us]` |
| `timestamp-millis`, `timestamp-micros` | `timestamp[ms, UTC]`, `timestamp[us, UTC]` |
| `local-timestamp-*` | `timestamp` with no zone |
| `decimal` | `decimal128(precision, scale)` |
| union with two or more non-null branches, `duration` | `user.sql.type_unsupported` at start |

`user.sql.type_unsupported` already exists: "A message field has a type the
handler cannot convert." The error names the field path and the Avro type.

JSON Schema to Arrow, draft 7 and 2020-12 vocabulary that the MVP reads:

| JSON Schema | Arrow |
| --- | --- |
| `string` | `utf8` |
| `integer` | `int64` |
| `number` | `float64` |
| `boolean` | `bool` |
| `object` with `properties` | `struct` |
| `array` with `items` | `list` |
| `type: [T, "null"]` | nullable `T` |
| `$ref`, `oneOf`, `anyOf`, `object` without `properties`, `array` without `items` | `user.sql.type_unsupported` at start |

`format: date-time` stays `utf8` in the MVP; a `CAST` in the handler SQL is
the workaround, and it is the same workaround the JSON path needs today.

### The typed handler

`handlers.New` gains a functional option, `WithDecoder(serde.Decoder)`. When
the option is set and the config names `handlers.InferredMemBatch`, the
builder returns a new `TypedBatchHandler` instead. From the outside it is the
same kind, `inferred_mem`, so no new handler type reaches the coverage registry
or the config schema.

`TypedBatchHandler` mirrors `InferredMemBatchHandler` in everything but the
decode. It holds one `array.RecordBuilder` over the decoder's schema plus the
metadata columns from `withMetadataFields`. `Write` calls `decoder.Append`, so
a record that does not decode fails at write time, which is the phase the
error policies key off and the reason `InferredMemBatch` validates JSON at
`Write` rather than `Invoke`. `Invoke` takes the record, binds it to the same
create-mode ingest statement into `batch`, runs the SQL, and reports
`RowsRead`. `Init` drops `batch`, as today.

### Encoding

The Kafka sink gains an `Encoder`. `WriteTable` calls `encoder.Encode` in
place of `tableRowsAsJSON`, and the `json` encoder is `tableRowsAsJSON`, so
the default path does not move.

The Avro and JSON Schema encoders derive an output schema from the batch's
Arrow schema, obtain a schema ID, and frame each row.

Obtaining the ID, cached by the Arrow schema's fingerprint so a stable SQL
result costs one registry call per run:

- `auto_register: true`: `CreateSchema` on the subject. The registry returns
  the existing ID for a schema it already holds, and 409 for one that breaks
  the subject's compatibility rule. 409 is `user.sink.schema_incompatible`.
- `auto_register: false`: `LookupSchema` on the subject. 404 is
  `user.sink.schema_unregistered`. This is the mode for a production registry
  where pipelines are not allowed to register.

Both codes are class `user`, so the retry ladder from #269 makes one attempt
and the process exits 10. That is the point: a SQL edit that changes the
output shape is stopped by the registry before any consumer sees it.

Arrow to Avro, the inverse of the table above with these rules: the record is
named from the subject with characters outside `[A-Za-z0-9_]` replaced by
`_`, in namespace `io.turbolytics.sqlflow`; a nullable field is
`["null", T]` with default `null`; `int8` and `int16` widen to `int`;
`uint8` through `uint32` widen to `long`; `uint64`, `large_utf8`,
`large_binary` and `dictionary` are `user.sink.type_unsupported`, which
already exists for the ClickHouse sink. Cell values come from the `arrowValue`
extractor the ClickHouse sink already has, which returns `time.Time` for
timestamps and dates, the types hamba expects for those logical types.

Arrow to JSON Schema: an `object` with one property per column, `required`
listing the non-nullable ones, and the type mapping inverted. The row bytes
are the JSON `tableRowsAsJSON` already produces, with the header prepended.

The sink implements `Prober`. `Probe` calls `Registry.Probe`, which lists
subjects with a short timeout, so a registry that is down fails the start once
with `system.sink.unreachable`, the way a ClickHouse that is down does.

### Error policy and the error class

`applyErrorPolicy` applies `IGNORE` and `DLQ` to every write error. Today that
is safe because handlers return only `user.data.malformed` from `Write`. The
decoder introduces the first system-class write error, a registry that stops
answering mid-run. Under `DLQ` policy that would divert good records to the
dead-letter queue and commit their offsets.

The fix is one guard: `IGNORE` and `DLQ` apply to class `user` only. A
system-class write error stops the pipeline whatever the policy, and the
process exits by the code's class, 11 for `system.source.unreachable`, which a
supervisor reads as retryable. `TestErrorDLQ_SystemClassWriteErrorIsNotDiverted`
pins it.

### New error codes

Appended to the registry, each with a summary and an action:

| Code | When | Action |
| --- | --- | --- |
| `user.data.schema_unknown` | A record's schema ID is not in the registry | Check the producer registers against the same registry the pipeline reads |
| `user.sink.schema_incompatible` | The registry refused the output schema under the subject's compatibility rule | Change the handler SQL to keep the output shape, or change the subject's compatibility level |
| `user.sink.schema_unregistered` | `auto_register` is false and the output schema is not registered | Register the schema, or set `auto_register: true` where the registry allows it |

`codes.golden` gains three lines. Everything else reuses `user.data.malformed`,
`user.sql.type_unsupported`, `user.sink.type_unsupported`,
`user.config.invalid`, `system.source.unreachable` and
`system.sink.unreachable`.

### Dev stack

`dev/kafka-single.yml` gains:

```yaml
  schema-registry:
    image: confluentinc/cp-schema-registry:7.3.2
    hostname: schema-registry
    container_name: schema-registry
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka1:19092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    depends_on:
      - kafka1
```

Same image line as the broker, so the two upgrade together.

### The framed-record producer

`cmd/publish-framed/main.go` registers a fixture schema under
`<topic>-value` and produces Confluent-framed records with franz-go and
hamba. It is the registry twin of `cmd/publish-test-data.py`, and four things
use it: the README walkthrough, the integration tests, the soak, and the
benchmark. The logic lives in `internal/serde/serdetest` so the tests import
it and the command is a thin wrapper.

Flags: `--format avro|json_schema`, `--brokers`, `--registry`, `--topic`,
and either `--num-messages N` for a fixed count or `--rate R --until
<unix-seconds>` for a steady stream. The fixture is the soak payload's shape,
`sensor_id` int, `ts` timestamp-millis, `value` double, so every soak and
benchmark config runs the same SQL whatever the format. `ts` is a real
timestamp in the typed formats and a string in raw JSON, which is the
difference the typed path exists to make.

`kafka-producer-perf-test` cannot produce these records: its payload file is
line-delimited text, and a framed Avro record is binary that can contain a
newline. So the producer runs inside the docker network the way the container
benchmark runs the engine: built as a linux binary in the `golang` image and
run in `debian:bookworm-slim` on `dev_default`, against `kafka1:19092` and
`http://schema-registry:8081`. From the host through Docker Desktop's port
forwarding it could not reach the rate the soak needs.

`make publish-framed FORMAT=avro TOPIC=orders NUM_MESSAGES=1000` wraps the
fixed-count mode for the walkthrough.

### Examples

Two configs under `dev/config/examples/`, every variable with a default so
the example test renders them:

- `kafka.avro.yml`: Avro in from the registry, aggregate in SQL, Avro out
  under a new subject with `auto_register: true`.
- `kafka.json-schema.yml`: JSON Schema in, JSON Schema out.

The example test builds the sink and the handler. The sink's `Probe` reaches
`localhost:8081`, and the test's `checkBuildError` skips on a connection error
and fails on "not supported" or "requires a". Messages from this change use
neither phrase for a resource that is merely absent.

### Tests

Coverage features, in `docs/coverage/features.yml`:

```yaml
  - id: serde.registry
    description: Resolves, caches, registers and looks up schemas in a Confluent-compatible registry.
    requires: [unit, integration]
  - id: serde.avro
    description: Decodes Confluent-framed Avro to typed Arrow and encodes Arrow batches back.
    requires: [unit, integration]
  - id: serde.json_schema
    description: Decodes Confluent-framed JSON Schema records to typed Arrow and encodes Arrow batches back.
    requires: [unit, integration]
```

Test names follow the registry's rule: `TestSerdeAvro_*`,
`TestSerdeJsonSchema_*`, `TestSerdeRegistry_*`, and
`TestIntegrationSerdeAvro_*` for the container pass. Release level is not
required for the MVP.

Unit, no network. A fake registry over `httptest.Server` implements the five
routes the client uses: schema by ID, latest version, create, lookup, and
subjects. It records calls so a test can assert the cache.

- Header: a record without the magic byte is `user.data.malformed`; a record
  with an unknown ID is `user.data.schema_unknown`; the same ID is fetched
  once across a thousand records.
- Avro to Arrow: one test per row of the mapping table, plus the two
  unsupported cases with their code and field path.
- Avro decode: a record with every supported type round-trips values; a
  writer schema with an added defaulted field and one with a removed field
  both decode against the reader; an incompatible type change is
  `user.data.malformed` naming both schemas.
- JSON Schema to Arrow: one test per row, plus the unsupported cases.
- Typed handler: `Write` of a bad record fails with the decoder's code and
  the batch still ingests the good ones; metadata columns are present and
  correct; `RowsRead` matches.
- Encoder: Arrow to Avro schema for every supported type and each
  unsupported one; `auto_register: true` calls create once per distinct
  schema; `auto_register: false` on an unregistered schema is
  `user.sink.schema_unregistered`; a 409 is `user.sink.schema_incompatible`;
  a framed record decodes back to the same values.
- Config: each rule in the Config section fails the build with
  `user.config.invalid` and a message naming the key.
- Error policy: a system-class write error under `DLQ` stops the pipeline
  and writes nothing to the DLQ.
- Registry: the append-only golden gains three lines; every new code exits
  10 and is not retryable.

Integration, one Redpanda container through
`testcontainers-go/modules/redpanda` v0.44.0, which ships a
Confluent-compatible registry in the same process as the broker. The test
fails rather than skips when it cannot start one, per the coverage rules.

- `TestIntegrationSerdeAvro_RoundTrip`: register a schema, produce framed
  records, run a pipeline with an Avro source and an Avro sink, consume the
  output topic, decode it with the schema the sink registered, and compare
  values and types.
- `TestIntegrationSerdeAvro_WriterSchemaEvolvesMidRun`: produce under version
  1, register version 2 with an added field, produce under it, and assert the
  pipeline keeps running and the added field reads as its default.
- `TestIntegrationSerdeJsonSchema_RoundTrip`: the same shape for JSON Schema.
- `TestIntegrationSerdeRegistry_DownAtStartFailsOnce`: a wrong port fails the
  start with `system.source.unreachable` and no retry ladder.

### Soak, one per format

The memory gate runs once per supported serialization format, and the PR
carries three verdict blocks:

| Format | Config | Producer |
| --- | --- | --- |
| raw JSON | `dev/config/soak/inferred.noop.yml`, unchanged | `kafka-producer-perf-test`, unchanged |
| JSON Schema | `dev/config/soak/json_schema.noop.yml` | `publish-framed --format json_schema --rate --until` |
| Avro | `dev/config/soak/avro.noop.yml` | `publish-framed --format avro --rate --until` |

`make soak SOAK_FORMAT=json|json_schema|avro`, default `json`.
`scripts/soak.sh` picks the config and the producer from the format and
passes `SQLFLOW_SCHEMA_REGISTRY_URL=http://schema-registry:8081` through
`SOAK_ENV` for the framed ones. Everything else is the existing script: the
same topic retention, the same deadline-driven producer loop, the same
sampler and verdict. The rate stays just under what the engine consumes, so
the gate is decided from the same producer position for every format.

All three keep the noop sink. The gate isolates the decode path on purpose:
what grows under Avro and not under raw JSON grew in the decoder, the
registry cache, or the typed handler, and nowhere else. The encoders are
covered by the benchmarks below, whose `allocs/op` is flat by construction
or the benchmark says so.

The raw JSON soak is the regression check. The decoder seam touches the
handler builder and the consume loop's write path, and the gate proves the
default path did not pick up an allocation on the way.

### Benchmarks, Go `testing.B` and an A/B against `main`

Two levels. The first is `go test -bench`, compared with `benchstat`. The
second is the container throughput run, which is the number the README
quotes.

Go benchmarks, in `internal/serde/bench_test.go` and beside the existing
handler benchmarks, each with `ReportAllocs` and `SetBytes` on the payload:

| Benchmark | Measures |
| --- | --- |
| `BenchmarkDecode/json`, `/json_schema`, `/avro` | One `Decoder.Append` per record over a 1,000-record fixture, registry pre-warmed |
| `BenchmarkEncode/json`, `/json_schema`, `/avro` | One `Encoder.Encode` over a 1,000-row batch, schema ID cached |
| `BenchmarkTypedBatch/json_schema`, `/avro` | Write plus Invoke through `TypedBatchHandler`, beside `BenchmarkInferredMemBatch` and `BenchmarkStructuredBatch` on the same fixture |

The A/B set is every benchmark that runs on both `main` and the branch:
`BenchmarkInferredMemBatch`, `BenchmarkStructuredBatch`,
`BenchmarkConsumeLoopWritePath`, and `BenchmarkDecode/json` plus
`BenchmarkEncode/json` once they exist on both. `scripts/bench-ab.sh
<baseline-ref>` checks the baseline out into a temporary worktree, runs the
set on both with `-count 10`, and prints `benchstat baseline.txt branch.txt`.
`benchstat` comes from `golang.org/x/perf/cmd/benchstat`, run with `go run`,
so nothing is installed.

Acceptance for the A/B: no benchmark in the set shows a statistically
significant regression. `benchstat` marks a delta with `p < 0.05`, and a
marked slowdown on the raw JSON path fails the PR. The typed formats have no
`main` counterpart; they are reported as absolute numbers and as a ratio to
`BenchmarkDecode/json` on the branch, so the cost of the typed path is a
number in the PR rather than a guess.

Container throughput, one run per format, `make benchmark-container
FORMAT=json|json_schema|avro`, with `benchmark.json-schema.mem.yml` and
`benchmark.avro.mem.yml` beside the existing benchmark configs and the
producer chosen by format. Two rounds each, same machine, in the PR as a
table with `main` as the first row, the shape #260 used.

### Acceptance

Against the dev stack:

```
make start-backing-services
make publish-framed FORMAT=avro TOPIC=orders NUM_MESSAGES=1000
./bin/sqlflow run dev/config/examples/kafka.avro.yml --max-msgs 1000
```

The pipeline exits 0. The output topic holds framed Avro records, and
`curl localhost:8081/subjects` lists both subjects. The console shows the
aggregate with typed columns, a `timestamp` not a string. Removing
`pipeline.schema_registry` from the config fails the start with
`user.config.invalid` naming the key. Stopping the registry container fails
the start with exit 11.

### Docs

README gains a "Schema registry" section under Sources: the config block, the
three formats, the two mapping tables, the sink modes, the error codes, and a
"Not yet" list that is the Out section of this spec. The `kafka` source and
sink examples reference it.

## What breaks if this is wrong

If the reader-schema-at-start rule is wrong, a producer that adds a field
mid-run does not surface it until the pipeline restarts. That is documented
and it is the Avro specification's own resolution behavior. A pipeline that
needs the new field restarts.

If the class guard on the error policy is wrong, a system-class write error
that used to be silently ignored under `IGNORE` now stops the pipeline. No
handler emits one today, so nothing in the wild changes behavior.

If the type mapping is wrong, a column reaches DuckDB with a type the SQL did
not expect. Every mapped type is in the unit tests, and every unmapped one
fails at start rather than at the first record.

## Build order

1. Config structs, rules, schema regeneration, error codes.
2. `serde.Registry` with the fake registry and its tests.
3. Avro to Arrow mapping and the Avro decoder, with `BenchmarkDecode/avro`.
4. `TypedBatchHandler`, the `WithDecoder` wiring through `run`, and
   `BenchmarkTypedBatch`.
5. Error policy class guard.
6. JSON Schema to Arrow mapping and decoder, with its benchmark.
7. Encoders and the Kafka sink wiring, Avro then JSON Schema, with
   `BenchmarkEncode`.
8. `serdetest` and `cmd/publish-framed`, the dev stack, examples, README.
9. Integration tests on Redpanda.
10. `scripts/bench-ab.sh`, the soak and benchmark configs and the format
    switches in `scripts/soak.sh` and `scripts/benchmark-container.sh`.
11. Run the gates: three soaks, the `benchstat` A/B, three container runs.
    Their output is the PR body.

Each step lands green on its own. The pipeline reads Avro after step 4.
