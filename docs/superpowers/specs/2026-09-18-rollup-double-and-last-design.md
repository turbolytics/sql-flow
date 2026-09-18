# `sqlflow rollup`: sums that keep their fraction, and a `last` measure

No issue yet. Verified against `main` at bc86bc7 on 2026-09-18. Part of
[Deploy to Render](2026-09-18-render-metrics-template-design.md). Extends
[Rollups](2026-09-15-rollups-design.md).

## The problem

The metrics template stores five values per minute per series: `sum`,
`count`, `min`, `max`, `last`. `sqlflow rollup` generates `sum`, `min`, `max`,
and `count_buckets`. Two things stand between it and the template.

A `sum` is cast to `bigint` in three places: the source expression and the
merge expression in `internal/rollup/sql.go` (lines 67 and 83), and the serve
SQL in `internal/rollup/serve.go` (line 132). The Bluesky demo counts posts,
so the cast is right there. A gauge of `0.73` summed through it is `0`.

There is no `last`. A gauge's headline value is its most recent one: queue
depth, open connections, a temperature. `gauge` is reserved in the measure
enum and refused, along with `avg` and `histogram`.

## Scope

In:

- `numeric: double` on a `sum` measure.
- A `last` measure type.
- Both in the generated migration, `rollup check`, the schema, and the rollup
  section of the README.
- `numeric: double` in the generated serve SQL.

Out:

- A generated serve dataset over more than one dimension, or without a fold.
  `rollup_check.go` allows a served dataset at most one dimension and
  requires a fold on it, and `grainSQL` is written to that shape. The metrics
  template has three dimensions and no fold, so it declares no `serve:` block
  and writes its dataset's grains by hand in `serve.yml`. Generalizing the
  generator is its own design.
- `last` in a generated serve dataset. It follows from the line above: the
  only shape the generator serves is a folded one, and `last` cannot fold.
- `avg`. A reader divides `value_sum` by `value_count`, which is exact at
  every grain. `avg` stays reserved.
- `gauge` and `histogram`. They stay reserved. `last` is a measure, not a
  metric type.
- `first`. Nothing needs it yet.
- `numeric` on `min` and `max`. They carry no cast today and take the source
  column's type.

## Decisions

| Decision | Choice | Rejected |
|---|---|---|
| How a sum keeps its fraction | An explicit `numeric: integer` or `double` on the measure, default `integer`. | Inferring from the source column's type. `sqlflow rollup` generates from the declaration alone and does not connect to a database. |
| Default | `integer`, today's `bigint`. | `double` for everyone. Every existing generated migration would drift and `rollup check` would fail in the Bluesky demo. |
| Postgres type for `double` | `double precision`. | `numeric`. Exact, but slower to merge and DuckDB reads it as a decimal whose width must be declared. |
| What orders `last` | The rollup's time column: the row from the latest finer bucket wins. | A separate `last_at` column per measure carried up the ladder. Buckets at one grain never overlap, so the bucket is already a total order. |
| `last` when a source minute is rewritten | The trigger re-merges the touched bucket from its finer rows, as every measure does. | Comparing against the stored value. A re-merge is already idempotent. |
| `last` and serve | A served dataset may not read a dimension set that has a `last` measure. The check refuses it. | Defining `last` of "other". A served dataset always folds, and the last value of forty unrelated series means nothing. |

## The change

### Config

`internal/config/rollup.go`, on `RollupMeasure`:

```go
// sum, min, max, last or count_buckets. avg, gauge and histogram are
// reserved and refused until a later version generates them.
Type string `yaml:"type" jsonschema:"enum=sum,enum=min,enum=max,enum=last,enum=count_buckets,enum=avg,enum=gauge,enum=histogram"`
// integer or double, for sum only. integer, the default, stores a bigint.
Numeric string `yaml:"numeric,omitempty" jsonschema:"enum=integer,enum=double"`
```

`internal/config/rollup_check.go`:

- `last` requires `column`, as `sum`, `min` and `max` do.
- `numeric` on any type but `sum` is a violation naming the field.
- A served dataset whose dimension set has a `last` measure is a violation.
  The message says to write the dataset in `serve.yml`.

### Generated Postgres

In `internal/rollup/sql.go`:

| | `sum`, integer | `sum`, double | `last` |
|---|---|---|---|
| Source | `sum(f.col)::bigint` | `sum(f.col)::double precision` | `(array_agg(f.col ORDER BY f.<time> DESC))[1]` |
| Merge | `sum(f.name)::bigint` | `sum(f.name)::double precision` | `(array_agg(f.name ORDER BY f.bucket DESC))[1]` |

`<time>` is the source's `time_column`. The aggregate runs inside the
existing `GROUP BY` of the coarser bucket and the dimensions, so it sees only
the finer rows of one bucket of one series: five for 1m to 5m, four for 6h to
1d.

Column types in the generated `CREATE TABLE`: `bigint`, `double precision`,
and for `last`, `double precision`. A `last` over an integer source column
widens; that is stated in the README and is the cost of generating without a
database connection. `numeric:` on `last` is refused rather than given a
second meaning.

A source row whose `last` column is null is skipped by ordering nulls last:
`ORDER BY f.bucket DESC` becomes `ORDER BY (f.col IS NULL), f.bucket DESC`.
A bucket whose every finer row is null stores null.

### Generated serve SQL

In `internal/rollup/serve.go`, the outer select gains one arm:

| Type | Expression |
|---|---|
| `sum`, integer | `sum(name)::BIGINT AS name`, unchanged |
| `sum`, double | `sum(name)::DOUBLE AS name` |

The switch has no `last` arm because the check refuses the dataset before
generation. The rank in a folded dataset reads a `sum` measure of either
numeric kind.

### `rollup check`

No new logic. It compares generated text with committed text, and the
generator now emits the new expressions.

## Compatibility

A declaration that uses neither addition generates byte-identical output. The
golden files under `internal/rollup/testdata` for existing cases must not
change, and that is the test. The Bluesky demo's `make validate` passes on
the new release without regenerating.

## Testing

- `internal/config`: `last` without `column` refused; `numeric` on `min`
  refused; `numeric: decimal` refused; a served dataset over a `last` measure
  refused; each names its path.
- `internal/rollup` golden: a new declaration with all five template
  measures, three dimensions and no `serve:` block generates the migration. A
  second, one dimension with a double sum and a fold, generates a serve
  dataset. Existing goldens unchanged.
- `internal/rollup/postgres_integration_test.go`, against a real Postgres:
  - `0.25 + 0.5` through a `double` sum reads `0.75` at every grain.
  - Minutes `10:00` = 3, `10:01` = 9, `10:04` = 4 give `last` 4 at 5m and up.
  - Rewriting `10:04` to 7 gives 7 at every grain; rewriting `10:01` to 100
    leaves `last` at 7.
  - Deleting `10:04` leaves the rollups unchanged, as for every measure.
  - A null `10:04` with a non-null `10:01` gives 9.
- `internal/rollup/serve_test.go`: the generated dataset SQL, run in DuckDB
  against an attached Postgres, returns a `DOUBLE` with its fraction for a
  double sum.
