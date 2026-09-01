# Changelog

## v1.0.0 — turbine, the Go engine

SQLFlow now ships a second engine: **turbine**, a Go rewrite of the Python
stream processor that reads the same configuration files. A `sqlflow.yml`
written for the Python engine is intended to run unmodified on turbine — same
YAML spec, same Jinja2 templating, same JSON Schema, same DuckDB SQL.

The Python engine (`sqlflow/`, `turbolytics/sql-flow`) is unchanged and still
maintained. turbine is the recommended engine for new pipelines.

### Why

- **Throughput**: ~793,000 msgs/sec versus the Python engine's tens of
  thousands on the same pipeline — roughly 20-60x.
- **Deployment**: a single binary, suitable for edge and IoT hardware.
- **Ergonomics**: background processing in Go rather than Python.

### Added

**Engine**
- Go pipeline: Kafka source (franz-go) → zero-copy JSON→Arrow → DuckDB via
  Arrow ADBC → sink, with batch accumulation and manual offset commits.
- `handlers.StructuredBatch` — schema declared up front, zero-copy Arrow ingest.
- `handlers.InferredMemBatch` — schema inferred from the JSON, matching
  `pyarrow.Table.from_pylist` semantics.
- `handlers.InferredDiskBatch` — disk-buffered via `read_json_auto` / `COPY TO`,
  with staged files cleaned up on close.
- `flush_interval_seconds` now actually bounds the batch wait (default 30s), so
  a low-traffic topic no longer stalls indefinitely.

**Sources**
- Kafka, with SASL/TLS on both source and sink: `PLAINTEXT`, `SSL`,
  `SASL_PLAINTEXT`, `SASL_SSL`; `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.
  GSSAPI and encrypted PEM keys are rejected with a clear error rather than
  silently ignored.
- Kafka metadata injection: `kafka_topic`, `kafka_partition`, `kafka_offset`.
- WebSocket, with reconnect and backoff (verified against the live Bluesky
  jetstream).
- Webhook: `POST /events` on `:8001` with HMAC-SHA256 validation and
  backpressure.

**Sinks**
- `console`, `kafka`, `sqlcommand`, `iceberg`, `clickhouse`, `noop`.
- `kafka` sink blocks on broker acks before offsets commit.
- `sqlcommand` + `substitutions` (`uuid4`) — the path to Postgres, S3 parquet,
  local parquet, DuckLake and MotherDuck.
- `iceberg` via apache/iceberg-go, resolving the catalog from `.pyiceberg.yaml`
  exactly as pyiceberg does.
- `clickhouse`, with an explicit column list derived from the Arrow schema.

**Operations**
- Error policies: `pipeline.on_error.policy` ∈ `RAISE` / `IGNORE` / `DLQ`, with
  the DLQ as a full nested sink, applied at both `handler.write` and
  `handler.invoke`.
- Tumbling window table managers, including a final poll on shutdown so a window
  closing during shutdown is not stranded.
- Prometheus metrics: seven OTel instruments on `:8000/metrics`, the same port
  the Python engine uses.
- Strict config parsing — an unknown key is an error, not a silently dropped
  setting.

**CLI and packaging**
- `turbine run` (`--max-msgs`, `--metrics`, `--stats-json`, `--pprof`),
  `dev invoke`, `config validate`, `config example`, `tail`, `version`.
- `Dockerfile.turbine` — multi-stage, cgo, libduckdb baked in.
- `make release-binaries` — linux and darwin on amd64 and arm64.
- DuckDB version pinned in one place, the `DUCKDB_VERSION` file.
- `make benchmark-container`, which runs the benchmark inside the docker network
  (host→container port-forwarding understates throughput ~10x).

### Fixed

- **Consume-loop data loss**: reaching `--max-msgs`, or a partial batch left when
  the source ended, discarded the batch in flight while still reporting those
  messages as consumed.
- **Fixture data corruption**: `bufio.Scanner` reuses its buffer and both
  handlers retain the caller's slice, so any fixture line larger than the read
  buffer produced corrupted rows.

### Not supported in turbine

- **UDFs.** Dropped by decision — they belong to DuckDB (a macro, an extension,
  or an `ATTACH`ed database). A `udfs:` block is a hard error naming the
  functions rather than a silent skip.
- **ClickHouse sink**: nested types, decimals and intervals are rejected with an
  explicit unsupported-type error.
- **Webhook source**: fragmented-message framing differs from the Python
  implementation, and the webhook's own request metrics are not ported.
- `--with-http-debug` and a configurable log level are not implemented.

See [Differences from the Python engine](README.md#differences-from-the-python-engine)
for the complete list, including CLI flag differences (`run -c <config>` and
`--max-msgs`, versus the Python engine's positional config and
`--max-msgs-to-process`).
