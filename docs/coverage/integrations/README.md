# Integrations

Every integration sqlflow can construct, and what each one claims. One file
per integration, named for its id: `sink.clickhouse.yml` declares
`id: sink.clickhouse`. A file whose name and id disagree fails
`make coverage-check`.

One file per case in `sinks.buildSink`, `sources.New` and `handlers.New`. A
test in each of those packages holds `Kinds()` equal to the ids here, so a new
case without a file fails `go test -short` in seconds. An integration the
engine can build and this directory does not name has no invariant cells at
all, which is the `sink.iceberg` failure: nothing written down, so nothing
could be missing.

Its status file has the same name, under `../status/`, so everything about one
integration is two files: what it declares, and where it stands.

## Fields

- `id` and `kind` are required. `kind` is one of `sink`, `source`, `handler`,
  `pipeline`, and it decides which invariants apply.
- `feature` ties the entry to `../features.yml`. Every shipped integration
  needs one.
- `implements` lists the interfaces the integration satisfies beyond its
  kind's base contract. `Prober` means the sink checks its destination before
  the first batch arrives.
- `exempt` names invariants that cannot apply, each with a `reason` and a
  `proven_by` naming a test. An exemption is one of two statements that must
  agree: the harness skips what it cannot exercise, and this file excuses it.
  Either one alone leaves the cell missing.
- `test_only` marks an id that ships to nobody. It gets no cells and needs no
  feature. The conformance harness's doubles use one, so a double's marker
  credits no real sink.
- `constructed: false` marks an entry no constructor switch builds. Pipeline
  configurations are the only ones, and the `Kinds()` agreement tests skip
  them.
- `types` and `nulls` declare what a sink does with each Arrow type in
  `../lattice.yml`. Only `sink.clickhouse` has them today. They are written by
  hand from reading the sink, never generated from it: a table derived from
  the sink can never report that the sink is missing a type, and the types it
  is missing are the point.

## Adding one

Write the file, name it for the id, and run `make coverage-check`. It reports
a missing `feature`, an exemption with no proof, a type key outside the
lattice, and a lattice key the type table does not answer for.
