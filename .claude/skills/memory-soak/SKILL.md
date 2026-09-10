---
name: memory-soak
description: Use when a sqlflow process grows over time, when a change touches Arrow, ADBC, DuckDB, or any cgo boundary, or before tagging a release. Also use when a heap profile shows nothing but the container keeps growing.
---

# Memory soak

Run a pipeline for twenty minutes under steady load and decompose where
its memory lives. The decomposition is the point. A Go heap profile
cannot see a buffer that DuckDB allocated across the ADBC boundary, and
`duckdb_memory()` does not track it either. Only the process can.

## The rule

**A flat heap profile does not mean no leak.** The leak this skill was
built to find grew a process from 48 to 90 MiB while `go tool pprof`
diffed to zero bytes over fifteen minutes. The memory was native.

Decompose every sample into three numbers, from `/proc/1/status` and
`runtime.MemStats`:

| number | source | what it is |
| --- | --- | --- |
| Go retained | `Sys - HeapReleased` | what the Go runtime holds |
| DuckDB tracked | `duckdb_memory()` on the live connection | what DuckDB accounts for |
| native | `RssAnon - Go retained` | everything else, and where cgo leaks land |

If native climbs while the other two are flat, the leak is a native
buffer whose Go handle was released or never existed. Arrow is reference
counted, so look for a `Retain()` without a matching `Release()`, and for
`Retain()` called on something a constructor already returned holding one
reference.

## Run it

```bash
.claude/skills/memory-soak/soak.sh <image> <config.yml> <minutes> <label>
```

The script starts the pipeline with `--pprof --metrics=prometheus
--with-http-debug`, samples the decomposition once a minute into
`soak-<label>/decomp.csv`, and saves a heap profile per minute. Feed the
topic yourself at a steady rate; a burst that drains in seconds measures
nothing. The producer in `producer.sh` paces 500 messages a second.

Then compare a baseline against a candidate:

```bash
.claude/skills/memory-soak/compare.py soak-v1.0.6/decomp.csv soak-fix/decomp.csv
```

It prints the native slope per minute for each, after a warm-up window,
and says whether each plateaued.

## Localize with controls

One soak says whether there is a leak. Controls say where. Run the same
image against the same topic with the pipeline reduced:

1. source to `InferredMemBatch` to `noop`, no `ATTACH`, no join
2. add the `ATTACH` and the join back, keep `noop`
3. the full pipeline with its real sink

Compare growth per message across the three, over the same message
window, not the same wall clock. A control replaying a backlog runs far
faster than one at steady state.

## Red flags

| Thought | Reality |
|---|---|
| "pprof shows nothing, so it's fine" | pprof shows Go. Check native. |
| "the benchmark says memory is flat" | The benchmark finishes in under a second. 44 bytes a message is invisible there. |
| "it's still climbing after the fix" | Warm-up lasts several minutes. Judge the slope from minute 8 on. |
| "the sink must be leaking" | Run it with `noop` first. The sink was innocent last time. |

## What settles it

A regression test, not a soak. A leak linear in messages needs message
volume, not wall clock: `internal/handlers/leak_test.go` pushes half a
million messages through a handler in under a second and asserts
resident memory does not grow. Write one of those for the path you fixed.
Keep the soak as a release gate for the class nobody wrote a test for.
