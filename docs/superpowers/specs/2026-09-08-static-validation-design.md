# Static validation: fail a broken pipeline before it runs

Issue: #169. Delivers #142. Related: #231 (`run` skips the schema `validate`
enforces), #161 (exit codes), #178 (Control API v1).

## The problem

Issue #120 is the only extended user engagement in this repo's history. Every
failure in it was catchable before the pipeline started, with tools already
linked into the binary. None of them were caught.

What went wrong, in order:

1. `commands:` written as a YAML map instead of a list. `config validate`
   catches this. The user never ran it, and `run` does not enforce the same
   schema (#231).
2. `ACCOUNT_NAME {{ VAR }}` unquoted, with a trailing comma before `)`. The
   rendered SQL is invalid DuckDB. It failed when the command executed at
   startup, inside a `CREATE SECRET`, with no indication which config line was
   at fault.
3. Docker Compose provided `SQLFLOW_AZURE_STORAGE_CONNECTION_STRING`. The
   config read `{{ SQLFLOW_AZURE_CONNECTION_STRING }}` — a different name. The
   template rendered it to an empty string with no error, producing
   `SET azure_storage_connection_string = '';`. The user chased an
   authentication failure that was a typo.
4. The image shipped DuckDB 0.9.2, which has no `CREATE SECRET`.

Failure 3 is the expensive one, and it is silent by construction.

## Evidence

Three facts, verified on 2026-09-08 rather than assumed:

| Check | Result |
|---|---|
| gonja renders an undefined variable | `err=<nil>`, output `SET conn = '';` — silent empty string |
| `json_serialize_sql('CREATE SECRET s (TYPE azure, ACCOUNT_NAME foo,);')` | `{"error":true,"error_type":"parser","error_message":"syntax error at or near \")\"","position":"46"}`, no execution |
| `PREPARE p AS SELECT nonexistent_col FROM t` | `Binder Error: Referenced column "nonexistent_col" not found` … `Candidate bindings: "a"` |

DuckDB parses and binds without executing, in the exact dialect and the exact
version that will run the pipeline. It is the linter. sqlfluff and sqlglot are
the wrong dialect and would return Python to a Go binary.

`gonja/v2` exposes `Template.Root()` for the parsed AST and accepts a
`config.Config` with `StrictUndefined`. Both are reachable.

## Who uses this

**A CI job.** #142 asks for exactly this and has no other supplier.

**An LLM.** Pipelines are increasingly model-generated. A model authors
plausible YAML quickly and is wrong in ways a human is not: confabulated config
keys, type values that do not exist, magic table names from a different tool,
invented environment variable names. It cannot read a browser. It needs one
command, every diagnostic at once, JSON, and positions it can patch.

**A human at a terminal**, running the loop PR #122 documents: validate, then
`dev invoke` against a fixture, then `run --with-http-debug`.

The browser-authoring persona is not evidenced in this repo and is deferred.

## Design principle: a false positive is worse than a miss

A model obeys its linter. Report a spurious error and it will "fix" correct
config until the error goes away, making a working pipeline broken.

So every check reports `pass`, `fail`, or `skipped` **with a reason**, and a
check that cannot run says so rather than guessing. A consumer must be able to
tell "checked and fine" from "not checked."

## Scope

In, and all of it offline — no broker, no sink, no sample, no side effects:

1. `sqlflow validate <config>` with `--json`: every diagnostic in one pass.
2. Template variable reporting: referenced, provided, missing, unused, with
   did-you-mean.
3. SQL parse and bind for every SQL string the config carries.
4. `sqlflow schema --json`: the grounding manifest.

Out of v1: `sqlflow test` and golden fixtures, covering the shipped examples
with them, an MCP adapter, `infer`, `preview`, `sample`, `sinkcheck`, and the
browser UI. Each is a follow-up issue.

## The checks

`validate` runs these in order and never stops at the first failure.

### 1. `config.template`

Parse the template. Walk `Template.Root()` and collect every variable
reference with its source position. Diff against the context `run` would
build: `SQLFLOW_`-prefixed environment variables, the settings vars, and
explicit overrides.

Report four sets: `referenced`, `provided`, `missing`, `unused`. Fuzzy-match
each missing name against the provided ones and emit `did_you_mean`.

Reporting `unused` is what makes failure 3 obvious. The report reads: missing
`SQLFLOW_AZURE_CONNECTION_STRING`, and you provide
`SQLFLOW_AZURE_STORAGE_CONNECTION_STRING` that nothing reads. The typo names
itself.

New code: `user.config.template_undefined`.

### 2. `config.schema`

Render the template, parse the YAML, validate against
`internal/cli/schemas/config.json`.

The schema declares `additionalProperties: false` in six places. Audit it for
completeness: an unknown key that validates silently is precisely how a
confabulated `pipeline.parallelism` survives to runtime.

Errors carry the instance path the validator already reports.

### 3. `config.registry`

Check that `source.type`, `handler.type`, and every `sink.type` exist, against
`sources.Kinds()`, `handlers.Kinds()`, and `sinks.Kinds()`. On failure, list the
valid values. A model that reads "sink type `postgres` is not known; valid
types are console, kafka, clickhouse, iceberg, sqlcommand, noop" repairs in one
turn.

### 4. `sql.syntax`

Parse every SQL string in the config through `json_serialize_sql`: each
`commands[].sql`, each `tables.sql[].sql`, `handler.sql`, the `sqlcommand`
sink's `sql`, and both tumbling-window statements.

Nothing executes. DuckDB returns the error type, the message, and a character
offset.

Positions map to the file by parsing the rendered YAML into a `yaml.Node` tree,
which carries `Line` and `Column` for every node. The SQL block's start line
plus DuckDB's offset gives a real file position.

New code: `user.sql.parse_failed`.

This check catches failure 2.

### 5. `sql.bind`

Open a throwaway in-memory DuckDB. Run `tables.sql` DDL into it. `PREPARE`
every remaining SQL statement against the resulting catalog.

`commands` do **not** run. `CREATE SECRET` reads credentials and `INSTALL`
reaches the network, and validate has no side effects. `--run-commands` opts
in, and is documented as executing the config's command block.

Bind coverage depends on where the schema comes from:

| Handler | Schema source | Bind |
|---|---|---|
| `StructuredBatch` | the declared table's DDL | full |
| `InferredMemBatch`, `InferredDiskBatch` | the data | skipped, reason recorded |

Skip rather than guess when a statement references a function or type that only
a `commands` block provides. Report `skipped` with the reason. This is where the
false-positive principle is load-bearing: an unbound `azure_*` function is not a
user error.

Reuses the existing `user.sql.bind_failed`, whose registry action already reads
"Check column names and types against a sample message."

### 6. `sql.conventions`

The magic names a model gets wrong:

- Handler SQL that reads from a table other than `batch` or one declared in
  `tables.sql`. The bind check reports this as a Catalog Error with DuckDB's own
  did-you-mean.
- A `sqlcommand` sink whose SQL never references `sqlflow_sink_batch`. Almost
  always a mistake; reported as a warning, not an error.
- Substitution variables declared but unused, or used but undeclared.

## Output

`--json` is the contract. Human output is a rendering of the same structure.

```json
{
  "config": "dev/config/examples/azure-debug.yml",
  "ok": false,
  "checks": [
    {"id": "config.template", "status": "fail"},
    {"id": "config.schema",   "status": "pass"},
    {"id": "config.registry", "status": "pass"},
    {"id": "sql.syntax",      "status": "fail"},
    {"id": "sql.bind",        "status": "skipped",
     "reason": "handler is InferredMemBatch; its schema comes from the data"},
    {"id": "sql.conventions", "status": "pass"}
  ],
  "diagnostics": [
    {
      "code": "user.config.template_undefined",
      "class": "user",
      "severity": "error",
      "message": "template variable SQLFLOW_AZURE_CONNECTION_STRING is not defined and renders as an empty string",
      "position": {"source": "config", "line": 14, "column": 41},
      "did_you_mean": ["SQLFLOW_AZURE_STORAGE_CONNECTION_STRING"],
      "action": "Define the variable, or correct the name."
    },
    {
      "code": "user.sql.parse_failed",
      "class": "user",
      "severity": "error",
      "message": "syntax error at or near \")\"",
      "position": {"source": "sql", "line": 9, "column": 7},
      "context": "commands[1].sql",
      "action": "Correct the statement. A trailing comma before a closing paren is the usual cause."
    }
  ],
  "variables": {
    "referenced": ["SQLFLOW_AZURE_CONNECTION_STRING", "SQLFLOW_KAFKA_BROKERS"],
    "provided":   ["SQLFLOW_AZURE_STORAGE_CONNECTION_STRING", "SQLFLOW_KAFKA_BROKERS"],
    "missing":    ["SQLFLOW_AZURE_CONNECTION_STRING"],
    "unused":     ["SQLFLOW_AZURE_STORAGE_CONNECTION_STRING"]
  }
}
```

`severity` separates `error` from `warning`, so CI can gate on errors while a
convention warning stays advisory.

Exit codes follow #161: zero when `ok`, a distinct non-zero for user errors, so
an agent branches without parsing text.

### Position honesty

Template diagnostics carry exact source positions, taken from the AST.

Schema and SQL diagnostics are found in the rendered document. When the template
contains no control structure that changes the line count, rendered and source
lines are identical and the position is reported as-is. Otherwise the position
is reported against the rendered document and flagged `"rendered": true`.
`sqlflow config render` prints that document, which also answers the question
the #120 user could not: did my variables actually substitute?

## The grounding manifest

`sqlflow schema --json` emits, from sources that already exist:

- the embedded config JSON Schema
- `sources.Kinds()`, `handlers.Kinds()`, `sinks.Kinds()`
- the magic names: `batch`, `sqlflow_sink_batch`
- the `sqlcommand` substitution types
- the error registry: every code with its `Summary` and `Action`

An agent reads this once and stops guessing at surface area. Nothing here is
new work beyond serialization.

## Shape of the code

```
internal/validate/
  validate.go     # Request -> Report, the only entry point
  report.go       # Report, Check, Diagnostic, Variables
  template.go     # AST walk, variable sets, did-you-mean
  schema.go       # JSON Schema, unknown-key audit
  registry.go     # source/handler/sink kinds
  sql.go          # json_serialize_sql parse, PREPARE bind, position mapping
  json_test.go    # Request and Report survive a JSON round trip
internal/cli/
  validate.go     # `sqlflow validate`, --json, --run-commands
  schema.go       # `sqlflow schema --json`
  config.go       # `config render` joins validate and example
```

`Validate(ctx, Request) (Report, error)` takes config **text**, never a path,
and returns a bounded, serializable report. That is the same constraint #178's
pull-based control plane imposes: instances sit behind NAT, so a control verb
must survive a queue. `json_test.go` holds it to that for the cost of twenty
lines, with no executor machinery built before there is a second caller.

`sqlflow config validate` keeps working and delegates to the new path.

## Coverage

`docs/coverage/features.yml`:

```yaml
  - id: validate.template
    description: Reports referenced, provided, missing, and unused template variables.
    requires: [unit]

  - id: validate.schema
    description: Validates a rendered config against the config JSON Schema.
    requires: [unit]

  - id: validate.sql
    description: Parses and binds every SQL string a config carries, without executing it.
    requires: [unit]

  - id: validate.manifest
    description: Emits the config schema, integration kinds, magic names, and error registry.
    requires: [unit]
```

Nothing is added to `docs/coverage/invariants.yml`.

An earlier draft of this spec declared three: `validate.no_side_effects`,
`validate.reports_every_diagnostic` and `validate.skip_is_explicit`. They do
not belong there. The invariant matrix describes what the engine holds true at
runtime, across every integration of a kind -- a sink that never loses a
buffered batch, a source that never commits an offset for a row the sink did
not take. Validation is tooling. It runs before the pipeline, touches nothing,
and has no integrations to hold the claim across.

The three properties are still enforced, by name, in
`internal/validate`: tests named `TestValidateNoSideEffects*`,
`TestValidateReportsEveryDiagnostic*` and `TestValidateSkipIsExplicit*`. The
last is the one that matters, and its reasoning belongs in this spec rather
than in a matrix cell: a model obeys its linter, so a check that cannot run
must report skipped with a reason and never pass.

The regression test for all of this is #120's config: the real file, with the
real typo, asserting the exact diagnostics.

## What breaks if this is wrong

`validate.skip_is_explicit` is the one to fear. A spurious error teaches a model
to edit correct SQL until the linter is quiet, which converts a working pipeline
into a broken one. A missed error costs a runtime failure the user already gets
today. The asymmetry is why skipping is a first-class result.

A validator that passes a config `run` then rejects is worse than none, which is
why #231 belongs with this work: `run` must enforce what `validate` enforces.

## Build order

1. `internal/validate` with the template and schema checks, plus the JSON
   report. Closes #120's failures 1 and 3, and supplies #142.
2. The SQL checks: parse, then bind, then conventions.
3. `sqlflow schema --json`, `config render`, and wiring `run` to the same
   schema (#231).

## Follow-ups

1. `sqlflow test`: fixtures plus expected rows, and every shipped example
   covered by it. Models copy examples, so a dead example is bad training data.
2. An MCP adapter over `Validate`, so an agent calls it as a tool.
3. `infer`, `preview`, and `sample` as design-time verbs, with preview honoring
   `batch_size` so stateful pipelines preview honestly.
4. `sinkcheck` against a live ClickHouse table.
5. The browser UI, if the human-authoring persona ever shows up.
