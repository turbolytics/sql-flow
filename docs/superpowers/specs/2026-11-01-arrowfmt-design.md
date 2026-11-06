# One owner for timestamp text

Approved in conversation on 2026-11-01. Spec, plan and code share this branch
and one PR.

## The problem

arrow-go 18.8.0 changed how an Arrow timestamp becomes text. Version 18.7.0
rendered every timestamp with `time.RFC3339Nano`, so a timestamp without a
zone ended in `Z`. Version 18.8.0 renders it without a zone and without `Z`.

SQLFlow never chose a format. Five call sites took whatever Arrow printed:

- `internal/sinks/rows.go`: `array.RecordToJSON` for Kafka and file rows, and
  `ValueStr` for the Kafka key.
- `internal/sinks/postgres_types.go`: `GetOneForMarshal` for list, struct and
  map columns.
- `internal/cli/run/debug.go`: `GetOneForMarshal`.
- `internal/serve/encode.go`: its own layout for a top-level timestamp, and
  `GetOneForMarshal` for everything nested.

The dependency bump in #460 broke `type.roundtrip` in the conformance suite
and in the Postgres integration test. A user would have seen a changed
`timestamp` string in every sink with no change on our side.

`serve` and the sinks also disagree. `serve` renders a zone-less timestamp
without `Z`. The sinks rendered it with `Z`.

## The decision

A timestamp without a zone is a UTC instant, and it renders with `Z`. This is
what the sinks shipped and what `type.timestamp.instant` pins, with the host
nine hours ahead of UTC. A timestamp with a zone keeps the zoned rendering the
sinks shipped, which `sink.postgres.yml` pins. `serve` changes to match for a
zone-less timestamp. Its top-level zoned rule, UTC, stays.

## The rule

`internal/arrowfmt` renders every Arrow value that reaches text.

| Arrow type | Text |
|---|---|
| `timestamp` without a zone, any unit | the instant in UTC, `time.RFC3339Nano`, so it ends in `Z` |
| `timestamp` with a zone, any unit | the instant in the column's zone, `time.RFC3339Nano`, so it ends in an offset such as `+09:00` |
| a container holding a timestamp (list, large list, fixed-size list, struct, map, dictionary) | the container's own shape, each timestamp rendered by the rule above |
| everything else | what Arrow renders today |

A type tree without a timestamp passes through to Arrow unchanged. Nothing
else changes shape.

## The surface

- New package `internal/arrowfmt` with `Marshal`, `Value`, `Text` and
  `RecordRow`.
- The five call sites above use it.
- A test fails on any direct `ValueStr`, `GetOneForMarshal` or `RecordToJSON`
  outside the package, so the next call site cannot reintroduce the defect.

## What breaks if this is wrong

- A sink that renders a zone-less timestamp without `Z` writes a different
  string than it did. ClickHouse, Postgres and Kafka consumers that parse the
  old string misread it.
- `serve` consumers that parse the zone-less string see `Z` appear. This is a
  visible change to the `serve` response and ships in the release notes.

## Out of scope

- Builders. Arrays that DuckDB and ADBC return never pass through our
  builders, so a builder layout cannot fix them.
- Timestamp parsing on input. `internal/eventtime` is unchanged.
- Changing the zone-less meaning to a wall-clock time. That is a separate
  decision with its own migration.

## Evidence

- Characterization tests run against arrow-go 18.7.0 first and pass.
- The same tests pass on arrow-go 18.8.0 after the call sites move.
- `type.roundtrip` and `type.timestamp.instant` pass in the conformance and
  Postgres integration suites.
