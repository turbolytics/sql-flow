# Schema registry MVP: Avro and JSON Schema through Kafka, both directions

Issue #272. Written against `main` at ae8fd28 on 2026-09-12. Re-verified
against `main` at dd51502 on 2026-09-15.

## The problem

Every byte the engine reads is assumed to be JSON, and every byte the Kafka
sink writes is JSON. A customer's topics carry Confluent-framed Avro. Nothing
in the config can say so, and nothing in the engine can decode it.

## Scope

In:

- Kafka source: `json` (today), `json_schema`, `avro`. The last two are framed
  by a Confluent-compatible schema registry.
- Kafka sink: the same three, in two modes. The sink either generates its
  output schema from the handler SQL and registers it, or writes against a
  registered version the config names.
- Type tables for five directions, declared per format and judged by tests:
  Avro to Arrow, Arrow to Avro, JSON Schema to Arrow, Arrow to JSON Schema,
  and Arrow to JSON. A loop table covers a value read in a format and written
  back out in it.
- A schema compatibility matrix. The read side covers writer and reader
  schema changes. The write side covers registry compatibility levels,
  measured against Confluent Schema Registry.
- A dead-letter row carries the failed record's bytes, its schema ID and its
  position. A registry failure stops the pipeline under any error policy
  instead of diverting good records.
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
- The schema GUID in a record header. As of Confluent Platform 8.1.1, a
  producer set with `value.schema.id.serializer=HeaderSchemaIdSerializer`
  writes a 16-byte schema GUID into a header and leaves the ID prefix out of
  the value. Confluent's deserializers check the header first and then fall
  back to the prefix. The sink writes the prefix, which consumers read either
  way. The source reads only the prefix, and `core.Message` carries no
  headers. So a header-GUID producer's records fail as `user.data.malformed`,
  and the error message names this as a possible cause.
- A Kafka dead-letter queue that republishes a failed record's original bytes
  as its value, with the error in Kafka headers, the way Kafka Connect's does.
  Dead-letter records below carry the bytes in a column instead, which works
  for every sink.
- Compatibility columns for registries other than Confluent's own, such as
  Redpanda, Apicurio and Karapace. Each is another column with its own image.
- Writing a schema that has references. A generated schema has none, and a
  specified schema's are resolved for reading, not rewritten.
- Arrow types of their own for the Avro logical types the decoder does not
  map: `timestamp-nanos`, `local-timestamp-nanos`, `big-decimal`, and `uuid`
  on a `fixed`. They read as their underlying type until then.

## Prior art

Flink is the closest thing to this design that ships: a SQL engine reading
and writing Confluent-framed records. Two of them were read on 2026-09-15,
open-source Apache Flink and Confluent Cloud for Apache Flink, and both
informed the decisions below.

What both do that this spec already does:

| Decision | Flink |
| --- | --- |
| One reader schema, fixed at start | Confluent Cloud pins the version when a statement is created. "existing statements don't pick up or forward these new fields to the sink tables" |
| A value that does not fit its column fails the record | "If an incoming JSON type cannot be safely converted to the target SQL type, the statement fails with a deserialization error" |
| A dead-letter row keeps the record's bytes | Its DLQ table's `source_record` holds "topic, partition, offset, timestamp, timestamp_type, headers, key (bytes), and value (bytes)" |
| The output is checked against the registered schema | "Flink checks the schema your statement produces against the schema already registered for the topic… Flink rejects the conversion" |
| Avro `enum` reads as a string | Both do. Confluent adds that a Flink schema cannot contain an enum field |
| A zoned timestamp is an instant, a local one is not | `timestamp-millis` and `timestamp-micros` become an instant; `local-timestamp-*` a local date-time |

Where Flink shaped a decision here:

- A nullable JSON Schema property reads from all three forms, because
  Confluent's writer emits `oneOf(Null, T)`. See Decoding.
- An unknown Avro logical type reads as its underlying type, which is the
  Avro specification's rule and Flink's behavior. See Decoding.
- Schema references are the registry client's job in Flink and ours here,
  because franz-go's client stops at the reference list. See Schema
  references.
- Reading a schema ID from a record header goes in Out, with Confluent
  Cloud's order: header, then the payload prefix.

Where this spec does not follow Flink:

- A union with two or more non-null branches fails at start. Open-source
  Flink falls back to a Kryo-serialized `GENERIC(Object)` that no SQL can
  read, and Confluent Cloud maps it to a `ROW` whose field naming its own
  documentation contradicts.
- The specified-schema mode registers nothing. Flink's
  `avro-confluent.schema` is "The schema registered or to be registered", so
  Flink still registers, and a read-only registry refuses that.
- The error policy covers a sink's failures too. Confluent Cloud's covers
  only the source: "Only deserialization errors at the source are
  supported."

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
        schema:                          # optional; absent means generate and register
          version: latest                # latest | a version number
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
- `subject` and `schema` are sink-only keys, and both require a `format`
  other than `json`.
- `schema.version` is `latest` or an integer of at least 1.
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
	Schema       *KafkaValueSchema `yaml:"schema,omitempty"`
}

// KafkaValueSchema names the registered version a sink writes against.
type KafkaValueSchema struct {
	Version string `yaml:"version" jsonschema:"oneof_type=string;integer,pattern=^latest$,minimum=1"`
}
```

`Version` is a string because `yaml.v3` decodes both `version: latest` and
`version: 3` into one, which was checked on 2026-09-15. The generated JSON
Schema has to accept both a string and an integer. `oneof_type` gives it both
types. `pattern` constrains only a string and `minimum` only an integer. `make
schema` regenerates the JSON Schema from these tags, and its golden file shows
the result.

`schema` replaces the draft's `auto_register`. The draft's `auto_register:
false` looked up the generated schema. A lookup only matches a schema byte for
byte after canonicalization, and no schema a team owns will match the one
sqlflow generates. With `schema` present, the sink registers nothing. Without
it, the sink registers what it generates. That is two modes and one key.

`config.Sink` is the shape of three sinks: the pipeline's sink, the DLQ, and
a window's sink. `KafkaSink.Value` reaches all three.

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
func (r *Registry) Version(ctx context.Context, subject string, version int) (sr.SubjectSchema, error)
func (r *Registry) Refs(ctx context.Context, s sr.Schema) ([]sr.SubjectSchema, error)

// Decoder turns one framed record into one row of the reader schema, in two
// phases. Decode makes every conversion and every check, and touches no
// builder. Append cannot fail. A record reaches the batch whole or not at all.
type Decoder interface {
	Schema() *arrow.Schema
	Decode(value []byte) (Row, error)
	Append(b *array.RecordBuilder, row Row)
}

// Row holds one value per field of Schema(), in field order. Each value is
// the Go type that field's builder appends, or nil for a null. A struct is a
// nested Row, a list is a []any, and a map is a []MapEntry.
type Row []any
func NewDecoder(ctx context.Context, format string, topic string, reg *Registry) (Decoder, error)

// Encoder turns a batch into framed records, one per row.
type Encoder interface {
	Encode(ctx context.Context, rec arrow.Record) ([][]byte, error)
}
func NewEncoder(ctx context.Context, format string, subject string, schema *config.KafkaValueSchema, reg *Registry) (Encoder, error)
```

`Version` calls `SchemaByVersion`, whose `-1` is the latest version, so
`latest` needs no second route. `Latest` stays for the source's reader schema.

The franz-go module `github.com/twmb/franz-go/pkg/sr` v1.8.0 supplies the
client and `ConfluentHeader`, whose `DecodeID` and `AppendEncode` are the
header. `github.com/hamba/avro/v2` v2.31.0 supplies Avro. Both are new
dependencies, and both versions are the latest release on 2026-09-15. arrow-go's `arrow/avro` package was considered and not used: it
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
   byte is `user.data.malformed`. Its message says the value has no schema ID
   prefix, and that a producer writing the schema GUID into a header sends
   records this way.
2. The writer schema is fetched by ID from the registry, through the cache.
   An ID the registry does not know is `user.data.schema_unknown`. A registry
   that does not answer is `system.source.unreachable`, and the fetch is
   retried three times over about a second before it is reported, because the
   alternative is a good record in the DLQ. A registry that answers 401 or
   403 is `user.source.security_invalid`, at start or mid-run.
3. Avro: hamba's `SchemaCompatibility.Resolve(reader, writer)` produces the
   resolved schema, cached per writer ID, and `avro.Unmarshal` decodes the
   payload into a `map[string]any` shaped like the reader. This is Avro schema
   resolution as the specification defines it: added fields take their
   defaults, removed fields are dropped, and a change the reader cannot
   resolve is `user.data.malformed` naming both schemas. A payload that does
   not decode against its own writer schema is `user.data.malformed`. `Decode`
   then converts each value to its column's Arrow value. A value that decodes
   and does not convert is `user.data.invalid`, which Value failures below
   lists.
4. JSON Schema: the payload after the header is JSON. `Decode` reads each
   property the Arrow schema names and converts it. No inference runs. A
   value that does not convert is `user.data.invalid`. A property the payload
   lacks is null, unless the schema lists it in `required`.
5. `Append` writes the converted row. Every check has already passed, so it
   has no error to return. Kafka metadata columns are appended by the
   handler, as today.

#### Value failures

A record can decode and still hold a value its column cannot take. Each such
value is `user.data.invalid`. That code exists and nothing returns it today.
Its summary is "A message is unusable for a reason the specific codes do not
cover." The message names the field path, the type the column wants, and what
the record held.

Avro's binary encoding follows the writer schema, so an Avro value always has
its declared type. What can still fail:

- A `decimal` whose unscaled value has more digits than the declared
  precision. Avro does not enforce precision on the wire.
- A timestamp outside the range of its Arrow unit.
- A `string` that is not valid UTF-8.

JSON Schema's payload is text the producer wrote, and nothing checked it
against the schema. What fails:

- A JSON type the column cannot take: a string for `integer`, an object for a
  scalar, a scalar for an `object` or an `array`.
- A number an `integer` column cannot hold: one with a fraction, or one
  outside `int64`.
- An explicit `null` in a property whose `type` does not include `null`.
- A missing property the schema lists in `required`.
- A string that is not valid UTF-8.

The JSON Schema checks stop there. `enum`, `const`, `pattern`, `minimum`,
`maximum` and `format` are not checked. A record the SQL can read is not
refused over a rule the SQL never uses.

`StructuredBatch` appends null for a value that does not parse as its
column's type, and it raises no error. The JSON Schema decoder does not reuse
that code. It has its own converter and refuses the value. `StructuredBatch`
keeps its behavior. `InferredMemBatch` already follows this rule: "a value
that cannot be promoted fails the batch rather than being silently nulled."

The UTF-8 check turns a batch failure into a record failure. On 2026-09-15,
through DuckDB 1.5.2's Python binding, one invalid UTF-8 string in an Arrow
batch failed the whole ingest:

```
CREATE TABLE batch AS SELECT * FROM src
Invalid Input Error: Invalid unicode (byte sequence mismatch) detected in segment statistics update
```

Without the check, one bad string fails `Invoke`. `IGNORE` or `DLQ` would then
drop or divert every good record in the batch, and the dead-letter row would
say only `Handler invocation failed`. The decoding runner confirms the failure
through ADBC.

Two phases, not an undo. A partial append leaves some column builders one
value longer than others, and undoing it means truncating every builder,
nested and variable-length ones included. Decoding the batch at `Invoke`
instead, as `StructuredBatch` does, fails the whole batch for one bad value.
Decoding at `Write` and again at `Invoke` doubles the decode cost. The two
phases cost one intermediate `Row` per record. `BenchmarkDecode` and the Avro
and JSON Schema soaks measure that cost.

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
| a logical type this table does not name | its underlying Avro type |
| union with two or more non-null branches, `duration` | `user.sql.type_unsupported` at start |

A logical type the decoder does not know reads as the type underneath it. The
Avro specification requires it: "Language implementations must ignore unknown
logical types when reading, and should use the underlying Avro type." Flink
does the same. Its `AvroSchemaConverter` returns `INT` for an `int` whose
logical type is neither `date` nor `time-millis`, and `BIGINT` for any other
`long`. So `timestamp-nanos` and `local-timestamp-nanos` read as `int64`,
`big-decimal` as `binary`, and `uuid` on a `fixed` as `binary`. Each is a
lattice row declaring that. Mapping them to Arrow types of their own is in
Out.

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
| `oneOf: [T, null]`, `anyOf: [T, null]`, in either order | nullable `T` |
| `$ref`, local or to another subject | the referenced schema's mapping |
| `oneOf` or `anyOf` with two or more non-null branches, `object` without `properties`, `array` without `items` | `user.sql.type_unsupported` at start |

A nullable property arrives in all three forms, so all three read as a
nullable `T`. Confluent's own writer uses the second: Confluent Cloud for
Apache Flink documents that "Nullable types are expressed as oneOf(Null, T)".
Refusing `oneOf` outright would refuse the schemas Confluent's tools
produce.

`format: date-time` stays `utf8` in the MVP; a `CAST` in the handler SQL is
the workaround, and it is the same workaround the JSON path needs today.

These two tables are the first draft of the `decode:` declarations in Type
tables below. Once the runner judges the declarations, they are the
authority.

#### Schema references

A registered schema can import another. Its registry entry carries a
`references` list, and each entry names a schema by name, subject and
version. The payload is unchanged: the referenced types are part of the
schema the ID resolves to.

Nothing resolves them for us. Flink's Confluent format hands the ID to
Confluent's Java client and takes back a parsed schema, so the client does
the resolving and Flink has no code for it
(`ConfluentSchemaRegistryCoder.readSchema` calls
`schemaRegistryClient.getById`). franz-go's client returns the schema and its
`References` list, and stops there. So `serde.Registry` resolves them:

1. `Refs` walks a schema's `References`, fetching each with `Version`, depth
   first, and returns the closure in dependency order.
2. Each fetch goes through the same cache as any other schema. A subject and
   version pair is immutable, so the cache never expires.
3. A cycle, or a closure deeper than 32, is `user.sql.type_unsupported` at
   start. A recursive record is already unsupported for the same reason.

Each format assembles the closure its own way:

- Avro: each referenced schema is parsed into a `hamba` `SchemaCache` under
  its name, in dependency order, then the root is parsed with
  `ParseWithCache`.
- JSON Schema: each referenced schema is added to the compiler with
  `AddResource` under the name the reference gives it, then the root is
  compiled. That covers a local `$ref` too, which needs no registry call.

The sink never writes a schema with references. A generated schema has none.
A specified schema's references are resolved the same way before the encoder
is built. Flink is the same in both directions: its writer calls
`register(subject, schema)` with no references.

### The typed handler

`handlers.New` takes no options today, and it has two callers: `run` and
`dev invoke`. It gains a functional option, `WithDecoder(serde.Decoder)`. When
the option is set and the config names `handlers.InferredMemBatch`, the
builder returns a new `TypedBatchHandler` instead. From the outside it is the
same kind, `inferred_mem`, so no new handler type reaches the coverage registry
or the config schema.

`TypedBatchHandler` mirrors `InferredMemBatchHandler` in everything but the
decode. It holds one `array.RecordBuilder` over the decoder's schema plus the
metadata columns from `withMetadataFields`. `Write` calls `decoder.Decode`,
and then `decoder.Append` only if `Decode` succeeded. A record that does not
decode fails at write time and appends nothing. Write is the phase the error
policies key off, and the reason `InferredMemBatch` validates JSON at `Write`
rather than `Invoke`. The metadata columns are appended after the decoded row,
and they cannot fail either. `Invoke` takes the record, binds it to the same
create-mode ingest statement into `batch`, runs the SQL, and reports
`RowsRead`. `Init` drops `batch`, as today.

### Encoding

The Kafka sink gains an `Encoder`. `WriteTable` calls `encoder.Encode` in
place of `tableRowsAsJSON`, and the `json` encoder is `tableRowsAsJSON`, so
the default path does not move.

The Avro and JSON Schema encoders have two modes. The `schema` key picks one.

**Generated**, with no `schema` block. The encoder derives an output schema
from the batch's Arrow schema, registers it with `CreateSchema` on the
subject, and frames each row with the returned ID. The ID is cached by the
Arrow schema's fingerprint, so a stable SQL result costs one registry call per
run. The registry returns the existing ID for a schema it already holds. It
returns 409 for a schema that breaks the subject's compatibility rule, and 409
is `user.sink.schema_incompatible`. The registry's credentials need write
access.

**Specified**, with a `schema` block. At start, the encoder fetches the named
version with `Registry.Version`. It parses the schema and frames every row
with that version's ID. The pipeline registers nothing, so read-only
credentials are enough. This is Confluent's own serializer with
`auto.register.schemas=false` and `use.latest.version=true`, the setting
Confluent documents for producers that must not register.

`latest` resolves once, at start, and stays fixed for the run. A version
registered mid-run is used after the next restart. This is the rule the source
already follows for its reader schema. A pinned version gives a repeatable
deploy.

The fetch runs in `Probe`, so each failure is reported at start:

- A registry that does not answer is `system.sink.unreachable`, exit 12.
- A subject or version the registry does not hold is
  `user.sink.schema_unregistered`, exit 10. `Probe` codes it, so `sinks.New`
  passes it through.
- A field whose type the encoding table declares unsupported for every Arrow
  key is `user.sink.type_unsupported`, naming the field path. A union with two
  or more non-null branches and a recursive record are two such fields.

Every user-class code here fails on the first attempt. The Kafka sink is not
wrapped in the #269 retry ladder, because franz-go retries a produce itself. A
`WriteTable` error returns from the batch, and the process exits 10. That is
the point: a SQL edit that changes the output shape is stopped before any
consumer sees it. #279 keeps the refused batch's offsets uncommitted, so a
restart replays it.

#### Matching the output to a specified schema

The handler SQL's output columns match the schema's top-level fields by name.
Names are compared exactly, including case, because Avro field names and
JSON Schema property names are case-sensitive. Field `aliases` play no part:
Avro defines them for a reader resolving an old writer, not for a writer.

| Output column vs schema field | Result |
| --- | --- |
| Column and field share a name | The value is encoded into the field's type, as the encoding table declares for that Arrow key and target type |
| Column with no field of its name | `user.sink.schema_mismatch`, naming the column |
| Field with no column, and the field declares a `default` | The default is written |
| Field with no column, and no `default` | `user.sink.schema_mismatch`, naming the field |
| Null into a field that is not a union with null | `user.sink.encode_failed`, naming the field |
| Columns in a different order from the fields | Written in the schema's order, with no effect on the record |

An extra column is refused, not dropped. A column the SQL computes and the
topic never receives is data loss, and nothing downstream reports it. To keep
the column out of the record, remove it from the SELECT.

A field with no column takes its `default` because Avro's own record builders
fill a missing field from its default and refuse a missing field that has
none. `["null", T]` with no `default` is refused like any other field without
one. JSON Schema has no record builder, so a JSON Schema property the output
lacks is left out of the object when it is not listed in `required`. It is
`user.sink.schema_mismatch` when it is.

A struct column matches a record field by the same rules, one level down. A
map's values and an array's elements are judged by the encoding table. A utf8
column bound for an `enum` field must hold one of the enum's symbols. Any
other value is `user.sink.encode_failed`. The enum's `default` is for readers,
so a writer does not apply it.

The match runs once per distinct output Arrow schema, before the first row of
that schema is encoded. It is cached by the Arrow schema's fingerprint, as the
generated mode caches its ID. A handler's output is not known before its first
batch, so the check cannot run at start. A batch that does not match writes
nothing. The check is a function of the Arrow schema and the registry schema
alone, so `validate` can run it once it binds the handler SQL against a
fetched schema, which is the follow-up in Out.

Arrow to Avro in the generated mode is the inverse of the Avro to Arrow table
in Decoding, with these rules: the record is
named from the subject with characters outside `[A-Za-z0-9_]` replaced by
`_`, in namespace `io.turbolytics.sqlflow`; a nullable field is
`["null", T]` with default `null`; `int8` and `int16` widen to `int`;
`uint8` through `uint32` widen to `long`; `uint64`, `large_utf8`,
`large_binary` and `dictionary` are `user.sink.type_unsupported`, which the
ClickHouse and Postgres sinks already return. Cell values come from the
`arrowValue` extractor the ClickHouse sink already has, which returns
`time.Time` for timestamps and dates, the types hamba expects for those
logical types. `arrowValue` covers booleans, integers, floats, strings,
binary, timestamps, dates and lists. It returns `user.sink.type_unsupported`
for decimal, struct, map and time columns, so the mapping above needs those
cases added.

Arrow to JSON Schema in the generated mode: an `object` with one property per
column, `required` listing the non-nullable ones, and the type mapping
inverted. The row bytes are the JSON `tableRowsAsJSON` already produces, with
the header prepended. In the specified mode, the same bytes are framed with
the named version's ID once the output matches it.

Both rule sets are the first draft of the `types:` declarations in Type tables
below.

The Kafka sink already implements `Prober`: `Probe` pings the seed brokers.
With a registry format, `Probe` also calls `Registry.Probe`, which lists
subjects with a short timeout. `sinks.New` classifies the probe's error, so a
registry that is down fails the start once with `system.sink.unreachable`,
exit 12, the way a ClickHouse that is down does.

### Type tables

Every type conversion a format makes is a declared table that a test judges.
The declaration is the published contract, the way `sink.clickhouse`'s type
table is (2026-09-07 ClickHouse type table design).

| Direction | Keys | Declared in | Judged by |
| --- | --- | --- | --- |
| Avro to Arrow | `lattice.avro.yml`, new | `serde.avro.yml`, `decode:` | `conformance.DecodeTypes`, new |
| JSON Schema to Arrow | `lattice.json_schema.yml`, new | `serde.json_schema.yml`, `decode:` | `conformance.DecodeTypes` |
| Arrow to Avro | `lattice.yml` | `serde.avro.yml`, `types:` | `conformance.Types` |
| Arrow to JSON Schema | `lattice.yml` | `serde.json_schema.yml`, `types:` | `conformance.Types` |
| Arrow to JSON | `lattice.yml` | `serde.json.yml`, `types:` | `conformance.Types` |

The lattices live in `docs/coverage/` and the declarations in
`docs/coverage/integrations/`.

The source's format and the sink's format are independent. Avro in with JSON
out is a valid pipeline, and so is JSON in with Avro out. The Arrow to JSON
table is what makes Avro in with JSON out safe to publish.
`tableRowsAsJSON` has never declared what it writes for a decimal, a
timestamp, a map or bytes, and a typed source now hands it all four. The same
table covers today's `json` Kafka sink and the console sink.

#### The `serde` integration kind

`serde` joins `sink`, `source`, `handler`, `pipeline` and `manager` in
`KINDS` and `INTEGRATION_KINDS` (`scripts/coverage_matrix/registries.py`).
Each format gets one file: `serde.avro.yml`, `serde.json_schema.yml` and
`serde.json.yml`. `serde.Formats()` lists the formats the engine builds. A
test holds that list equal to the `serde` ids, the agreement `handlers.Kinds()`
already has.

The integration is the format, not the Kafka source or sink. The encoder and
the decoder convert types. The Kafka client moves bytes, and `source.kafka`
and `sink.kafka` already cover that. The MQTT sink (#195) reuses the same
formats.

An invariant's `applies_to` accepts a list. The six existing type invariants
apply to `[sink, serde]`, unchanged.

A format exempts an invariant it cannot exercise, with a reason and a test,
as the integration rules require. `serde.json` has no decoder of its own: the
JSON path's decoder is `InferredMemBatch`'s inference, which
`handler.inferred_mem` covers. So `serde.json` exempts the decoding
invariants, `type.loop` and both compatibility invariants.
`serde.json_schema` exempts `type.decode.timestamp.instant` while
`format: date-time` stays `utf8`.

#### Encoding tables

The encoding tables are keyed by `lattice.yml`, so every Arrow type DuckDB can
hand an encoder has an answer. `conformance.Types` runs them unchanged.
`serdetest.EncoderSink` adapts an `Encoder` to `core.Sink`:

- `WriteTable` encodes the batch and keeps the framed bytes. `Flush` does
  nothing.
- `Prepare(key, columnType)` registers a record with one field, `v`, of type
  `columnType` in the fake registry. It returns an `EncoderSink` that encodes
  against that record.
- `ReadBack` decodes the kept bytes with hamba against the registered schema,
  and renders `v`.

A row's `columns` are the target types a specified schema can give that key.
`int64` lists `long` and `["null", "long"]`. `utf8` lists `string`, a
`uuid` string with a UUID `value`, and an `enum` with a symbol as its `value`.
The row is covered only when every listed target passes, as with a ClickHouse
column list.

Each row also declares `emits`: the type the generated schema gives the key.
The runner checks the registered field against it.

Nulls need two additions to the runner. An Avro `long` has no way to hold a
null, and `["null", "long"]` holds one exactly. So a column entry may carry
its own `null:` rule, overriding the integration's `nulls.default`. `NullRule`
also gains a third outcome, `refused`, with a `code`. A null bound for a field
that is not a union with null is refused with `user.sink.encode_failed`.

#### Decoding tables

A decoder's input is not Arrow, so `lattice.yml` cannot key it. Two new
lattices close the input sets, under the same rules as `lattice.yml`:

- The set is closed.
- A declared key outside the set is a typo.
- A key a format does not answer for is a gap.

The ClickHouse type table design deferred this boundary as "a separate
column". These lattices are those columns.

`lattice.avro.yml` spells a key the way Avro spells the type. A logical type
is `<underlying>:<logicalType>`.

| Tier | Keys |
| --- | --- |
| Primitives | `null`, `boolean`, `int`, `long`, `float`, `double`, `bytes`, `string` |
| Logical types | `int:date`, `int:time-millis`, `long:time-micros`, `long:timestamp-millis`, `long:timestamp-micros`, `long:timestamp-nanos`, `long:local-timestamp-millis`, `long:local-timestamp-micros`, `long:local-timestamp-nanos`, `bytes:decimal`, `fixed:decimal`, `bytes:big-decimal`, `string:uuid`, `fixed:uuid`, `fixed:duration`, `long:unknown` |
| Named types | `enum`, `fixed`, `record` |
| Unions | `["null", T]`, `[T, "null"]`, `[A, B]`, `["null", A, B]` |
| Containers | `array<T>` for every depth-1 key the decoder maps; `array<fixed:duration>` and `array<[A, B]>` as unsupported-element witnesses; `map<long>`, `map<string>` |
| Depth 3 | `array<array<long>>`, `array<record>`, `record<record>` |
| References | a recursive `record`; a named type imported through the registry's `references` |

Three of these rows have their outcome settled in Decoding above. A reference
resolves. A recursive record is unsupported. A logical type the decoder does
not map reads as its underlying type.

hamba v2.31.0 knows ten logical types. The Avro 1.12 specification adds
`timestamp-nanos`, `local-timestamp-nanos`, `big-decimal` and `uuid` on a
16-byte `fixed`. A producer can write all four, so each is a key.
The specification also says: "Language implementations must ignore unknown
logical types when reading, and should use the underlying Avro type."
`long:unknown` pins that rule.

`lattice.json_schema.yml`:

| Tier | Keys |
| --- | --- |
| Scalars | `string`, `integer`, `number`, `boolean`, `null` |
| Formats and encodings | `string:date-time`, `string:date`, `string:time`, `string:uuid`, `string:base64` |
| Enumerations | `enum`, `const` |
| Nullability | `type: [T, "null"]`, `oneOf: [T, null]`, `anyOf: [T, null]` |
| Containers | `array<T>` for every depth-1 key the decoder maps, plus two unsupported-element witnesses; `object` with `properties`; `object` with only `additionalProperties`; `object` with neither; `array` with no `items`; tuple `prefixItems` |
| Composition | local `$ref`, `$ref` through the registry's `references`, `allOf`, `anyOf`, `oneOf`, `not`, `if`/`then`/`else`, `patternProperties` |
| Depth 3 | `array<array<integer>>`, `array<object>`, `object<object>` |

`oneOf: [T, null]` and `anyOf: [T, null]` are their own keys because schema
generators write a nullable field in either form, not only as `type: [T,
"null"]`.

Each `decode:` row declares four things:

- `outcome`: `exact`, `coerced` with a `rule`, or `unsupported` with a `code`.
- `arrow`: the Arrow key the decoder builds, as `conformance.CanonicalKey`
  spells it. The key does not have to be in `lattice.yml`, which lists what
  DuckDB emits. `int:time-millis` builds `time32[ms]`, which DuckDB reads and
  never writes.
- `duckdb`: the type of column `v` in `batch`, as `typeof(v)` reports it.
- `expect`: `v::VARCHAR`.

`conformance.DecodeTypes` runs each row in four steps:

1. It registers a record with one field of the key's type in the fake
   registry.
2. It frames one record holding the key's canonical value.
3. It writes the record through `TypedBatchHandler`.
4. It reads `typeof(v)` and `v::VARCHAR` from `batch`.

The canonical values live in a Go table beside `latticeType`. A test holds
that table's keys equal to the lattice file, as `LatticeKeys` does. The runner
goes through DuckDB and does not stop at the Arrow builder, because handler
SQL binds against DuckDB's type.

The decoding invariants are in family `types` and apply to `serde`:

| Invariant | Claim |
| --- | --- |
| `type.decode.roundtrip` | Every declared input type reaches `batch` with the declared Arrow key, DuckDB type and value. |
| `type.decode.null` | A null in every declared nullable form reaches `batch` as NULL. |
| `type.decode.nested` | Arrays, maps, records and their nesting reach `batch` intact, or are declared unsupported. An unsupported element fails from inside a supported container. |
| `type.decode.timestamp.instant` | A timestamp reaches `batch` as the same instant while both the host clock and the DuckDB session zone are off UTC. |
| `type.decode.string.fidelity` | The fidelity corpus reaches `batch` byte for byte. |
| `type.decode.unsupported.fails_at_start` | Every key declared unsupported fails at pipeline start, with the declared code and the field path, before any record is read. |

The last invariant replaces the Arrow side's undeclared-type probe. The input
schema is known at start, so the failure moves from the batch to the start.

#### The loop

A pipeline that reads Avro and writes Avro takes each value through four
conversions: decode, ingest into DuckDB, the query result, and encode. Each
conversion has its own table, and a value exact in every table can still
arrive changed.

Measured on DuckDB v1.5.2, the engine's pinned version, on 2026-09-15:

| Avro in | Decoder builds | `batch` column | Query returns | Generated schema writes |
| --- | --- | --- | --- | --- |
| `long:timestamp-millis` | `timestamp[ms, tz=UTC]` | `TIMESTAMP WITH TIME ZONE` | `timestamp[us, tz=<session>]` | `long:timestamp-micros` |
| `long:local-timestamp-millis` | `timestamp[ms]` | `TIMESTAMP_MS` | `timestamp[ms]` | `long:local-timestamp-millis` |
| `int:time-millis` | `time32[ms]` | `TIME` | `time64[us]` | `long:time-micros` |

`serde.avro.yml` and `serde.json_schema.yml` each declare a `loop:` block,
keyed by their input lattice. Each row declares two outcomes:

- through the generated schema;
- through a specified schema equal to the input schema.

`TestSerdeAvro_Loop` runs every key end to end:

1. Frame a record.
2. Decode it and ingest it.
3. Run `SELECT v FROM batch`.
4. Encode the result.
5. Decode the output with hamba and compare it to the input.

A key whose decoding and encoding rows are both `exact` must come back
`exact` through the specified schema. A key that does not is a finding.
Invariant `type.loop` applies to `serde`: "Every declared input type written
back out reads as declared."

#### Rendered pages

`scripts/coverage_matrix/page.py` renders `serde-avro-types.mdx` and
`serde-json-schema-types.mdx` from the declarations, as it renders
`clickhouse-types.mdx`. A decoding table is keyed by input type and shows the
DuckDB type a user's SQL sees. An encoding table is keyed by DuckDB SQL type,
through `lattice.yml`'s `duckdb` field, and shows the type written. The docs
site's schema registry page includes both pages, so the published table and
the tested table cannot disagree.

### Schema compatibility matrix

The type tables fix one schema. The compatibility matrix declares what happens
when a schema changes under a running pipeline, on both sides, and tests prove
each cell.

The change cases are a closed set per format, in
`docs/coverage/compat.avro.yml` and `docs/coverage/compat.json_schema.yml`.
The YAML carries the case ids. The before and after schemas live in Go, in
`internal/serde/serdetest`, and a test holds the two equal. That is the
lattice's arrangement.

The two invariants are in a new family, `compat`, added to `FAMILIES`. They
apply to `serde` and are verified by a new verifier, `compattable`, added to
`VERIFIERS`:

- `compat.read`: every declared schema change reads as declared.
- `compat.write`: every declared change to the output registers or is refused
  as declared.

#### Read side

Each case runs in two directions:

- **Writer newer.** A producer registers a new version mid-run. Records
  arrive under it, and the reader is still the version fixed at start.
- **Reader newer.** The pipeline restarts after the upgrade and replays a
  backlog written under the old version.

Avro cases:

- `field.add.default`, `field.add.no_default`
- `field.remove.default`, `field.remove.no_default`
- `promote.int_long`, `promote.int_float`, `promote.int_double`,
  `promote.long_float`, `promote.long_double`, `promote.float_double`,
  `promote.string_bytes`, `promote.bytes_string`
- `narrow.long_int`, `narrow.double_float`
- `field.rename.alias`, `field.rename.no_alias`
- `enum.symbol.add`, `enum.symbol.remove`, `enum.default`
- `union.branch.add`, `nullable.add`, `nullable.remove`
- `logical.change`, from `timestamp-millis` to `timestamp-micros`
- `logical.drop`
- `record.nested.field.add`, `record.rename`

JSON Schema cases:

- `property.add`, `property.remove`, `property.retype`
- `required.add`, `required.remove`
- `additional_properties.close`, `additional_properties.open`

Each cell declares one outcome:

- `resolved`, with the `rule` a reader sees and the `expect` value of the
  changed field in `batch`.
- `record_error`, with the `code` each record fails with. The error policy then
  applies to that record.

Under Value failures, `property.retype` is a `record_error` with
`user.data.invalid` whenever an old record's value does not convert to the
new type. It is `resolved` when the value does convert, as an integer does to
`number`. `required.add` is a `record_error` for a reader-newer backlog whose
records lack the property.

The read side runs at unit level. hamba's `SchemaCompatibility.Resolve`
decides Avro resolution. The JSON Schema extraction is ours. No registry
decides a read-side cell. `TestSerdeAvro_Compat` and
`TestSerdeJsonSchema_Compat` run the cells through `TypedBatchHandler` with the
fake registry.

#### Write side

The write side covers what the registry does when the handler SQL's output
changes, at each compatibility level: `BACKWARD`, `BACKWARD_TRANSITIVE`,
`FORWARD`, `FORWARD_TRANSITIVE`, `FULL`, `FULL_TRANSITIVE` and `NONE`.

The output changes to the generated schema:

- `column.add`, `column.drop`, `column.rename`, `column.reorder`
- `column.widen` (`int32` to `int64`), `column.narrow`, `column.retype`
- `inferred.all_null`: a JSON source's batch in which one field is null in
  every row. Inference types that field from nulls alone, so the generated
  schema changes between two batches with no SQL edit.

Each cell declares `registered`, or `refused` with
`user.sink.schema_incompatible`.

A specified schema is a separate column. The pipeline registers nothing in
that mode, so no compatibility level applies. Its cells judge the same output
changes against the named schema, by the matching rules in Encoding. Each cell
declares `exact`, `coerced` with a rule, or `refused` with a code. The
expected answers:

- `column.add` is refused with `user.sink.schema_mismatch`.
- `column.drop` writes the field's default, or is refused when the field has
  none.
- `column.rename` fails on both counts. The new name is an extra column, and
  the old field has no column.
- `column.reorder` is exact.
- `column.widen`, `column.narrow` and `column.retype` follow the encoding
  table's row for the new Arrow key and the field's type.

One case applies to this column alone. In `version.latest.moves`, a new
version is registered while the pipeline runs. The run keeps encoding against
the version it resolved at start, and records carry that version's ID until a
restart.

The write side runs at integration level against Confluent Schema Registry,
`confluentinc/cp-schema-registry:8.3.1`, the current release on 2026-09-15.
The registry's own compatibility checker decides these cells, so a fake
cannot stand in for it. Confluent's registry is the reference
implementation. Every other Confluent-compatible registry is another column
with its own image, and none is in the MVP. `TestIntegrationSerdeAvro_Compat`
and `TestIntegrationSerdeJsonSchema_Compat` set each level on a fresh subject,
register the before schema, and attempt the after schema through the sink's
encoder.

The JSON Schema write cells also depend on the generated schema's content
model. Confluent checks an open content model differently from a closed one,
one with `additionalProperties: false`. The generator's choice decides
whether `column.add` registers under `BACKWARD`. The matrix measures the
generator as it is written.

`page.py` renders `serde-avro-compat.mdx` and `serde-json-schema-compat.mdx`
beside the type pages.

### Error policy and the error class

`applyErrorPolicy` applies `IGNORE` and `DLQ` to every handler error, from
`Write` and from `Invoke` (`turbine.go:735` and `turbine.go:1194`).
`InferredMemBatch` returns `user.data.malformed` from `Write`.
`InferredDiskBatch` returns the same failure with no code, and no `Invoke`
failure carries a code. `errs.CodeOf` reports an error with no code as
`system.internal.unexpected`, which is class `system`.

The decoder adds write errors that say nothing about the record: a registry
that stops answering, or one that refuses the pipeline's credentials. The
record is intact. `IGNORE` would drop it, `DLQ` would divert it, and both
commit the offset past it. A brief registry outage would become permanent
loss.

The guard decides by the error code's domain, not its class. The class cannot
draw the line. An error with no code is class `system`, and on `main`
`InferredDiskBatch`'s invalid JSON and every `Invoke` failure carry no code.
A guard keyed on class would stop pipelines that ignore or divert those
failures today. `errs.Retryable` cannot draw it either. It takes an exit code,
and it returns true for exit 1, where an error with no code exits.

The rule: a handler error in domain `source` stops the pipeline whatever the
policy. That domain means something the source depends on failed. The
decoder returns two such codes:

- `system.source.unreachable`, a registry that does not answer after its
  retries. Exit 11, which a supervisor reads as retryable.
- `user.source.security_invalid`, a registry that refuses the credentials.
  Exit 10. That code's summary covers TLS and SASL today. Its summary and
  action gain "or the schema registry refused its credentials".

A record's own faults stay in domain `data`: `user.data.malformed` and
`user.data.schema_unknown`. The policy applies to them as it does today.

No handler on `main` returns a `source` code, so every failure that `IGNORE`
or `DLQ` handles today is still handled the same way.
`TestErrorDlq_SourceDomainWriteErrorIsNotDiverted` pins the guard under
`DLQ`, and `TestErrorIgnore_SourceDomainWriteErrorStops` pins it under
`IGNORE`. `TestErrorIgnore_UncodedInvokeErrorStillIgnored` pins today's
behavior.

### Dead-letter records

A dead-letter row today has four text columns: `error`, `message`, `phase`
and `timestamp`. The README documents that shape. On a write failure,
`message` holds the record's value as a string (`turbine.go:735`).

A framed record is not text. On 2026-09-15 a framed Avro record went through
`array.RecordToJSON` on arrow-go v18.6.0, which `tableRowsAsJSON` uses for the
Kafka and console sinks:

```
record:            00 00 00 00 07 02 04 61 ff
as a utf8 column:  "    a�"
as a binary column: "AAAAAAcCBGH/"
```

As text, `ff` became U+FFFD, and the record cannot be recovered. As binary,
the JSON writer emits base64, which decodes to the same bytes. A Postgres
dead-letter queue fails outright. Every framed record starts with `0x00`, and
a Postgres text column refuses a NUL byte. The DLQ write fails and
`applyErrorPolicy` returns the failure, so the pipeline stops at the first bad
record.

The dead-letter row keeps its four columns and gains five:

| Column | Arrow type | Holds |
| --- | --- | --- |
| `value` | `binary` | The record's value, byte for byte as consumed |
| `schema_id` | `int32` | The ID in the value's header, for a registry format |
| `topic` | `utf8` | The record's topic |
| `partition` | `int32` | The record's partition |
| `offset` | `int64` | The record's offset |

Each new column is null where it has no answer:

- A `handler.invoke` row describes a batch, not a record, so all five are
  null, as today's `message` is a fixed string.
- A source with no positions, such as a webhook or a websocket, has no topic,
  partition or offset.
- `schema_id` is null for format `json`, and when the value has no header to
  read.

`message` keeps today's content for format `json`. For `avro` and
`json_schema` it is null, and the bytes are in `value` alone. A string copy
would be the lossy one above, and it would carry a NUL byte into Postgres.

`applyErrorPolicy` takes the `core.Message` instead of its value as a string,
so `writeDLQ` can fill the new columns. `core` does not parse a
Confluent header itself. `run` passes `core.WithDLQSchemaID(serde.SchemaID)`
when the source's format is a registry format. Without that option,
`schema_id` stays null. A malformed JSON record that happens to start with
`0x00` is never read as a schema ID.

Every sink that can hold a dead-letter row already declares the new types.
Postgres declares `binary` as `bytea` and ClickHouse declares it as `String`,
both `exact`. `int32`, `int64` and `utf8` are exact in both. Kafka and
console render `binary` as base64, which `serde.json`'s type table will
declare.

A dead-letter table in ClickHouse or Postgres needs the five columns before
the upgrade. Both sinks insert by the batch's column names. ClickHouse builds
its column list from the Arrow schema. Postgres copies into a staging table
built from the batch's columns and merges from there. A table without the new
columns refuses the row, and a refused DLQ write stops the pipeline. The
README's four-column description, the `kafka.dlq.yml` example and the release
notes all state the new shape. A Kafka dead-letter topic's JSON rows gain five
keys, and a consumer that ignores unknown keys is unaffected.

`TestErrorDlq_FramedRecordKeepsItsBytes` writes an undecodable framed record
under `DLQ` and asserts that the row's `value` equals the input and that
`schema_id` is the header's ID. `TestIntegrationSerdeAvro_PostgresDlqKeepsTheRecord`
does the same through a Postgres dead-letter table and reads the `bytea`
back. The pipeline keeps consuming after the bad record.

### New error codes

Appended to the registry, each with a summary and an action:

| Code | When | Action |
| --- | --- | --- |
| `user.data.schema_unknown` | A record's schema ID is not in the registry | Check the producer registers against the same registry the pipeline reads |
| `user.sink.schema_incompatible` | The registry refused the output schema under the subject's compatibility rule | Change the handler SQL to keep the output shape, or change the subject's compatibility level |
| `user.sink.schema_unregistered` | The subject or version that `schema` names is not in the registry | Register the schema, or name a version the subject holds |
| `user.sink.schema_mismatch` | The handler SQL's output columns do not match the specified schema's fields | Rename, add or remove columns in the handler SQL to match the schema, or name a version that matches |

`codes.golden` gains four lines. Everything else reuses `user.data.malformed`,
`user.data.invalid`, `user.sql.type_unsupported`, `user.sink.type_unsupported`,
`user.sink.encode_failed`, `user.config.invalid`,
`user.source.security_invalid`, `system.source.unreachable` and
`system.sink.unreachable`. `user.source.security_invalid` keeps its code, and
its summary and action gain the schema registry's credentials.

### Dev stack

`dev/kafka-single.yml` gains:

```yaml
  schema-registry:
    image: confluentinc/cp-schema-registry:8.3.1
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

This is the image the compatibility matrix is measured against, so the
walkthrough runs on the implementation the matrix describes. The registry's
Kafka client talks to the stack's `cp-kafka:7.3.2` broker over the Kafka
protocol, so the two need not share a release.

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
  under a new subject with a generated schema.
- `kafka.json-schema.yml`: JSON Schema in, JSON Schema out.

The example test builds the pipeline's sink, each window's sink, and the
handler. It calls `handlers.New` with no options. The sink's `Probe` reaches
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
  - id: serde.json
    description: Encodes Arrow batches as one JSON object per row, the default format.
    requires: [unit]
```

Invariants, in `docs/coverage/invariants.yml`: the six decoding invariants,
`type.loop`, `compat.read` and `compat.write`, from Type tables and Schema
compatibility matrix above. The six existing type invariants change
`applies_to` from `sink` to `[sink, serde]`. `make coverage-check` gains
three checks:

- Every `decode:` key is in its format's input lattice.
- Every `compat:` case is in its format's case file.
- Every lattice key and every case is declared by every `serde` integration
  it applies to, or reported as a gap.

The gate attributes a test to a feature by its marker alone
(`scripts/coverage_matrix/features.py`). Every test calls
`coverage.Covers(t, "serde.avro")`, or its feature's id, first in its body,
before any skip. A test without the marker is reported as unattributed, and a
skipped test covers nothing. The comments in `features.yml` and
`internal/coverage/coverage.go` still describe attribution by name, which the
gate no longer does. Names keep the existing shape: `TestSerdeAvro_*`,
`TestSerdeJsonSchema_*`, `TestSerdeRegistry_*`, and
`TestIntegrationSerdeAvro_*` for the container pass. Release level is not
required for the MVP.

Unit, no network. A fake registry over `httptest.Server` implements the five
routes the client uses: schema by ID, latest version, create, lookup, and
subjects. It records calls so a test can assert the cache.

- Header: a record without the magic byte is `user.data.malformed`; a record
  with an unknown ID is `user.data.schema_unknown`; the same ID is fetched
  once across a thousand records.
- Decoding tables: `TestSerdeAvro_DecodeTypes` and
  `TestSerdeJsonSchema_DecodeTypes` run `conformance.DecodeTypes` over each
  format's `decode:` table.
- Encoding tables: `TestSerdeAvro_Types`, `TestSerdeJsonSchema_Types` and
  `TestSerdeJson_Types` run `conformance.Types` over each format's `types:`
  table.
- The loop: `TestSerdeAvro_Loop` and `TestSerdeJsonSchema_Loop`.
- Read side of the compatibility matrix: `TestSerdeAvro_Compat` and
  `TestSerdeJsonSchema_Compat`. An incompatible change names both schemas
  in its error.
- Row atomicity:
  - A record whose last field fails `Decode` appends nothing.
  - The next record's values land in their own row.
  - A batch holding one invalid UTF-8 string ingests every other record.
- Schema references:
  - A schema importing another resolves, and both formats decode a record
    holding the imported type.
  - Each referenced schema is fetched once across a thousand records.
  - A cycle fails at start with `user.sql.type_unsupported`.
  - `TestIntegrationSerdeAvro_ResolvesReferences` registers a schema with a
    reference against the real registry and reads a record through it.
- Nullable forms: `type: [T, "null"]`, `oneOf: [T, null]` and
  `anyOf: [T, null]` all decode to the same nullable column, in either
  branch order.
- Unknown logical types: `timestamp-nanos`, `local-timestamp-nanos`,
  `big-decimal` and `uuid` on a `fixed` each decode as the underlying type
  their lattice row declares.
- Value failures: each case in Value failures, for both formats, is
  `user.data.invalid` naming its field path. A missing optional property is
  null. A `pattern` mismatch is not refused.
- Typed handler: `Write` of a bad record fails with the decoder's code and
  the batch still ingests the good ones; metadata columns are present and
  correct; `RowsRead` matches.
- Encoder, generated mode: create is called once per distinct output schema;
  a 409 is `user.sink.schema_incompatible`.
- Encoder, specified mode:
  - `latest` and a pinned version each fetch once, at start, and frame
    records with that version's ID.
  - A missing subject or version is `user.sink.schema_unregistered` from
    `Probe`.
  - Each row of the matching table has a test with its code and the column or
    field it names.
  - A field default is written when its column is absent.
  - The match runs once per distinct output schema.
- Encoder, both modes:
  a framed record decodes back to the same values.
- Config: each rule in the Config section fails the build with
  `user.config.invalid` and a message naming the key.
- Error policy: a `source`-domain write error stops the pipeline under
  `DLQ` and under `IGNORE`, and writes nothing to the DLQ. An uncoded
  `Invoke` error under `IGNORE` is still ignored.
- Dead-letter rows:
  - A framed record's `value` equals its bytes, and `schema_id` equals its
    header's ID.
  - `message` is null for a registry format, and today's string for `json`.
  - A `handler.invoke` row has null in all five new columns.
- Registry: the append-only golden gains four lines; every new code exits
  10 and is not retryable.

Integration tests run against one broker and one registry. The broker is
`confluentinc/confluent-local:7.5.0` through `testcontainers-go/modules/kafka`,
the way the Kafka conformance test runs it: on a test network, under the alias
`kafka`. The registry is `confluentinc/cp-schema-registry:8.3.1`, a generic
container on the same network. The write side of the compatibility matrix is
measured against this registry, so the round-trip tests and the matrix
always run against the same implementation. No new testcontainers module is
needed. A test fails rather than skips when it cannot start either
container, per the coverage rules.

- `TestIntegrationSerdeAvro_RoundTrip`: register a schema, produce framed
  records, run a pipeline with an Avro source and an Avro sink, consume the
  output topic, decode it with the schema the sink registered, and compare
  values and types.
- `TestIntegrationSerdeAvro_WriterSchemaEvolvesMidRun`: produce under version
  1, register version 2 with an added field, produce under it, and assert the
  pipeline keeps running and the added field reads as its default.
- `TestIntegrationSerdeJsonSchema_RoundTrip`: the same shape for JSON Schema.
- `TestIntegrationSerdeAvro_WritesAgainstSpecifiedSchema`: register an output
  schema the way a topic's owners would. It has its own record name and
  namespace, `doc` strings, and a defaulted field the SQL does not produce.
  Run a pipeline naming it with `version: latest`. Then assert three things:
  - The subject still holds one version.
  - Every record carries that version's ID.
  - A consumer decoding with that schema reads the SQL's values and the
    field's default.
- `TestIntegrationSerdeAvro_GeneratedSchemasRegister` and
  `TestIntegrationSerdeJsonSchema_GeneratedSchemasRegister`: every schema the
  generator emits for a `types:` row registers against the real registry. The
  registry parses a schema and validates its names when the schema is
  registered, and the fake registry does neither.
- `TestIntegrationSerdeAvro_Compat` and `TestIntegrationSerdeJsonSchema_Compat`:
  the write side of the compatibility matrix.
- `TestIntegrationSerdeRegistry_DownAtStartFailsOnce`: a wrong port fails the
  start once, with no retry ladder. `run` probes the sink before it builds
  the handler. A pipeline with a registry-backed sink reports
  `system.sink.unreachable`, exit 12. A pipeline whose only registry format
  is on the source reports `system.source.unreachable`, exit 11.

### Soak, one per format

The memory gate runs once per supported serialization format, and the PR
carries three verdict blocks:

| Format | Config | Producer |
| --- | --- | --- |
| raw JSON | `dev/config/soak/inferred.noop.yml`, unchanged | `kafka-producer-perf-test`, unchanged |
| JSON Schema | `dev/config/soak/json_schema.noop.yml` | `publish-framed --format json_schema --rate --until` |
| Avro | `dev/config/soak/avro.noop.yml` | `publish-framed --format avro --rate --until` |

`make soak SOAK_FORMAT=json|json_schema|avro`, default `json`, beside the
`SOAK_MINUTES` and `SOAK_LABEL` it takes today. `scripts/soak.sh` names
`inferred.noop.yml` and runs `kafka-producer-perf-test` today. It picks the
config and the producer from the format and
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
| `BenchmarkDecode/json`, `/json_schema`, `/avro` | One `Decode` and one `Append` per record over a 1,000-record fixture, registry pre-warmed |
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

Container throughput, one run per format. `make benchmark-container` already
selects the pipeline with `CONFIG`, and the benchmark configs live in
`dev/config/examples/`. `benchmark.json-schema.mem.yml` and
`benchmark.avro.mem.yml` go beside them. The script produces with
`cmd/publish-test-data.py` today; a new `FORMAT` picks the producer, as in
`make benchmark-container CONFIG=dev/config/examples/benchmark.avro.mem.yml
FORMAT=avro`. Two rounds each, same machine, in the PR as a
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
the start with `system.sink.unreachable`, exit 12: `run` probes the sink
before it builds the handler, and this example's sink is registry-backed.

### Docs

README gains a "Schema registry" section under Sources: the config block, the
three formats, the sink modes, the error codes, and a "Not yet" list that is
the Out section of this spec. The `kafka` source and sink examples reference
it. The type tables and the compatibility matrix are not copied into the
README. It links to the rendered pages, which the docs site publishes.

The README's Error policies section changes in two ways:
- It states the dead-letter row's nine columns, and the upgrade step for an
  existing ClickHouse or Postgres dead-letter table.
- It states that an error from the source's registry stops the pipeline
  whatever the policy.

`kafka.dlq.yml` stays as it is. Its Kafka dead-letter topic takes the new
keys with no configuration change.

## What breaks if this is wrong

If the reader-schema-at-start rule is wrong, a producer that adds a field
mid-run does not surface it until the pipeline restarts. That is documented
and it is the Avro specification's own resolution behavior. A pipeline that
needs the new field restarts.

The sink's `latest` follows the same rule. A topic's owners who register a new
output version see pipelines write it after their next restart, not at once.
A pipeline that picked it up mid-run would change its records' shape with no
deploy, and no SQL edit would be reviewed against the new version.

If refusing an extra column is wrong, a pipeline whose SQL selects a helper
column stops at its first batch instead of dropping the column quietly. The
error names the column, and removing it from the SELECT is the fix.

If the domain guard is wrong, a handler error in domain `source` that
describes the record would stop a pipeline that should have diverted it. The
decoder returns two `source` codes, and neither describes a record. A future
handler that returns a `source` code for a bad record breaks this rule, and
the guard's tests name the rule it breaks.

If the dead-letter columns are wrong, an existing ClickHouse or Postgres
dead-letter table refuses the new row and the pipeline stops. The upgrade
note tells operators to add the columns first, and a table without them fails
the first bad record, not silently.

If refusing a value is wrong, a producer whose records break their own schema
sees them fail instead of reading as null. The error policy decides what
happens next. Under `DLQ` the record's bytes are kept, and the error names the
field. Before this change, such a record reached SQL with a null that nothing
reported.

If the type mapping is wrong, a column reaches DuckDB with a type the SQL did
not expect. Every input type is a lattice row that the decoding runner judges
through DuckDB. Every row declared unsupported fails at start, which
`type.decode.unsupported.fails_at_start` proves.

If the compatibility matrix is wrong, a schema change that the published page
calls safe stops a pipeline or diverts its records. The read-side cells run
the decoder the pipeline runs. The write-side cells run Confluent's own
compatibility checker. A Confluent-compatible registry that checks
differently is outside what the matrix measured.

## Build order

1. Config structs, rules, schema regeneration, error codes.
2. `serde.Registry`, including `Refs` and its cycle and depth limits, with the
   fake registry and its tests.
3. The declarations, with nothing judged yet:
   - the `serde` integration kind and `applies_to` lists;
   - `lattice.avro.yml`, `lattice.json_schema.yml` and the two compatibility
     case files, with their Go value tables;
   - the new invariants and the generator checks.
   The matrix reports every row as missing.
4. Avro to Arrow mapping and the Avro decoder, with `BenchmarkDecode/avro`.
5. `TypedBatchHandler`, the `WithDecoder` wiring through `run`,
   `BenchmarkTypedBatch`, `conformance.DecodeTypes`, and the `serde.avro`
   `decode:` table.
6. The error policy's domain guard, the five dead-letter columns, and
   `core.WithDLQSchemaID`.
7. JSON Schema to Arrow mapping and decoder, its `decode:` table, and its
   benchmark.
8. Read side of the compatibility matrix, both formats.
9. Encoders and the Kafka sink wiring, Avro then JSON Schema, with
   `serdetest.EncoderSink`, the runner's null additions, the three `types:`
   tables, and `BenchmarkEncode`.
10. The loop tables.
11. `serdetest` fixtures and `cmd/publish-framed`, the dev stack, examples,
    README, and the rendered pages.
12. Integration tests on Kafka and Confluent Schema Registry, including the
    write side of the compatibility matrix.
13. `scripts/bench-ab.sh`, the soak and benchmark configs and the format
    switches in `scripts/soak.sh` and `scripts/benchmark-container.sh`.
14. Run the gates: three soaks, the `benchstat` A/B, three container runs.
    Their output is the PR body.

Each step lands green on its own. The pipeline reads Avro after step 5.
