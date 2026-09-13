# `run` enforces the schema that `validate` enforces

Issue #231. Verified against `main` at a3da41e on 2026-09-12.

## The problem

`sqlflow run` loads a config without checking it against the config schema.
`sqlflow validate` and `sqlflow config validate` both do. A config that
`validate` rejects still runs.

The issue's example is a missing `batch_size`. The engine passes the parsed
value straight through, so `Turbine.batchSize` is 0. The consume loop closes a
batch with `numBatchMessages == t.batchSize`, and the count is at least 1 when
that runs, so the count-based flush never fires. Batches close only on
`flush_interval_seconds`, which defaults to 30 seconds. The issue measured
first output at 31 seconds instead of 0. Nothing in the logs names the cause.

The situation on `main` is worse than the issue describes. PR #241 replaced the
hand-written schema with one reflected from the config structs. `BatchSize`
carries `yaml:"batch_size,omitempty"`, and the reflector reads `omitempty` as
optional. The v1.0 schema listed `batch_size` under `pipeline.required`; the
reflected schema lists only `source`, `handler` and `sink`. So today `validate`
accepts the config too, and no command catches the missing key.

Three facts, each verified on `main`:

| Fact | Where |
| --- | --- |
| `run` calls `config.LoadRendered` and never `validate.Validate` | `internal/cli/run/root.go:137` |
| `pipeline.required` is `[source, handler, sink]` | `internal/validate/schemas/config.json` |
| `batch_size` of 0 makes the count flush unreachable | `internal/core/turbine.go:694` |

The README still says `batch_size` is required and at least 1. The schema no
longer says so, and the engine does not check.

## The change

Two parts. The first is the issue's ask. The second is the regression that
made the first insufficient.

### 1. `run` validates before it loads

`run` reads the config file, calls `validate.Validate` with the file's text,
and stops on any error-severity diagnostic. Only then does it render and load
the config as it does today.

Behavior on a failing config:

- Every error diagnostic is written to stderr in the same text format
  `validate` uses, so the two commands print the same lines for the same
  fault.
- The command returns `user.config.invalid`, which exits 10. A supervisor
  reads that as terminal and does not restart into the same failure.
- Nothing has started: no DuckDB, no source, no sink, no metrics server.

Warnings do not stop the run. They are written to stderr as warnings. The
one warning class today is an unsupplied template variable with no default.
`validate` demotes the resulting schema error to a warning because an
incomplete shell is not a wrong config. Under `run`, the shell is the
deployment, so an unsupplied variable is a real fault. Failing on it is a
policy change with its own blast radius and is out of scope here. The
warning line makes the empty substitution visible, which is more than `run`
says today.

`sqlflow dev` and `sqlflow tail` load a config the same way `run` does and
have the same gap. Both get the same call. One helper in `internal/cli`
performs the read, validate, and report, and the three commands call it. The
helper is the only place that decides what stops a run, so the commands
cannot drift.

`validate.Validate` renders the template once and `LoadRendered` renders it
again. Rendering is cheap and happens once per process. Sharing the rendered
bytes between the two is a refactor of `config.LoadRendered` for no
observable gain, so the double render stays.

### 2. `batch_size` is required again, and at least 1

The struct tag changes from `yaml:"batch_size,omitempty"` to
`yaml:"batch_size" jsonschema:"minimum=1"`. The reflector then lists
`batch_size` under `pipeline.required` and emits `"minimum": 1`, the same
way the Kafka fetch bounds already do. `make schema` regenerates the committed
schema and the `config example` golden.

Dropping `omitempty` also changes how a `Conf` marshals: a zero `BatchSize`
now serializes as `batch_size: 0`. Nothing in the engine marshals a `Conf`
back to YAML except the golden test, which is regenerated. The tests that
build a `Conf` in Go are unaffected, because they never go through the
schema.

Requiring the key rather than defaulting it is the issue's option 1. It
matches what the v1.0 schema declared, what the README documents, and what
the Python engine required. Option 2 needs a default, and the only defensible
one is 1, which is the slowest setting and would surprise anyone who lost the
line to a bad merge in the opposite direction.

The engine itself does not change. A defensive `>=` in the consume loop would
make 0 mean "flush every message", which is a second silent behavior for the
same mistake. The schema is the one place the rule lives.

### Tests

Schema:

- `internal/schema`: `pipeline.required` contains `batch_size`, and
  `batch_size` carries `minimum: 1`. This is the assertion that would have
  caught #241's regression. It sits beside the golden test rather than inside
  it, so a future regeneration cannot update it away.
- `internal/validate`: a config without `batch_size` fails `config.schema`
  with a diagnostic at `/pipeline`. A config with `batch_size: 0` fails with a
  diagnostic at `/pipeline/batch_size`.

CLI:

- `internal/cli/run`: `run` on a config missing `batch_size` returns an error
  whose code is `user.config.invalid`, and the message names `batch_size`.
  The test asserts that the error returns before the DuckDB open, by running
  against a config whose source would fail to connect if reached.
- `internal/cli/run`: `run` on a config with an unsupplied template variable
  and everything else valid proceeds past validation. This pins the
  warnings-do-not-stop rule.
- `internal/cli`: `dev` and `tail` on the same invalid config return the same
  code. One table-driven test over the three commands.
- `internal/cli/examples_test.go` already loads every example config. It
  gains a validate pass, so an example that the schema rejects fails the
  suite. Every example on `main` sets `batch_size`, so this is a guard, not a
  fix.

### Acceptance

The issue's own measurement, repeated:

```
$ sqlflow run pipeline-without-batch-size.yml
pipeline-without-batch-size.yml:3:1: error: [user.config.invalid] at '/pipeline': missing property 'batch_size'
Error: [user.config.invalid] pipeline-without-batch-size.yml is invalid
$ echo $?
10
```

And `validate` on the same file prints the same diagnostic line.

### Docs

README line 309 says an unset `flush_interval_seconds` means only
`batch_size` triggers a batch. `flushIntervalFor` defaults it to 30 seconds,
and the v1.0.0 changelog says so. The line changes to state the default.
Line 308 already says `batch_size` is required and at least 1, and becomes
true again.

The v1.2.1 changelog entry names the defect: `run` accepted configs that
`validate` rejected, and `batch_size` had become optional in the schema
without anyone deciding it should be.

## What breaks if this is wrong

A config in the wild that omits `batch_size` stops starting. On `main` that
config runs with a 30 second flush and no count-based batching, which is
almost never what its author meant. The error names the key and the fix is one
line. That is the trade the issue asks for.

A config that `validate` rejects for a reason `run` tolerated is the same
story. Strict YAML decoding already rejects unknown keys, so the new rejections
are type, enum, minimum and required violations. Each of those was a config
the engine ran with a value it did not expect.

## Out of scope

- Failing `run` on an unsupplied template variable. Noted above as a policy
  change with its own blast radius.
- SQL validation under `run`. #142 and #169 cover the SQL checks, and they are
  not part of `validate` yet.
- Sharing the rendered bytes between `validate` and `LoadRendered`.
