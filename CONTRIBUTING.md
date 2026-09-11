# Contributing

## Verification conventions

A pull request carries evidence, not assertions. "Tests pass" is not evidence;
the command and its output are. Every claim in a PR body should be one a
reviewer could re-run.

Five checks. The first four run on every PR. The fifth runs when the change
could touch memory, and the rule for that is below.

### 1. The Go suite, with the race detector

```bash
go test -short -race ./...
```

Race detection is not optional. Two races in the conformance harness reached
`main`, one of which panicked a CI run outright, and neither reproduced by
re-running.

### 2. The tooling suite

```bash
uv run --locked pytest tests/tooling -q
```

The coverage generator gates every merge, so a wrong generator is a wrong
gate.

### 3. The coverage gate

```bash
make coverage-page && git status --short docs/coverage
```

Clean means the committed page matches the status files and the registries. A
new feature needs an entry in `docs/coverage/features.yml` and a marker on the
tests that prove it, or the gate reports it missing.

### 4. The release suite, when the artifact changes

```bash
make sqlflow-image
SQLFLOW_IMAGE=turbolytics/sql-flow:$(git describe --tags --always --dirty) \
  uv run --locked pytest tests/release -q
```

Required when the change touches the CLI surface, a flag, the Dockerfile, or
anything stamped at build time. The image is what users run, and a broken
entrypoint or a missing `libduckdb` passes every unit test.

### 5. The memory soak

```bash
make start-backing-services
make sqlflow-image
make soak
```

Ten minutes against a saturated topic, polling `/turbostats/v1` once a second,
sampling the memory decomposition once a minute. It prints PASS or FAIL and
the numbers behind it.

**Required when the change touches** Arrow, ADBC, DuckDB, cgo, the consume
loop, a handler, a sink, the metrics recording path, or anything that
allocates per message or per request. When in doubt, run it: ten minutes is
cheaper than the alternative.

**Paste the verdict block into the PR body.** Not "memory is flat" — the
window, the message count, and the bytes per message.

#### Why this shape

Go's heap profiler cannot see a buffer DuckDB allocated across the ADBC
boundary, and `duckdb_memory()` does not track it either. Only the process
can. A leak that grew a pipeline from 48 to 90 MiB diffed to zero bytes in
`go tool pprof` over fifteen minutes.

The judgement is **bytes retained per message**, not megabytes per minute. A
leak is linear in messages, so per-message growth is comparable between a
saturated run and a trickle, and between a fast machine and a slow one. The
leak this gate exists to catch was 44 bytes a message: invisible in a
benchmark that finishes in a second, and 90 MiB over a day in production. The
threshold is 1 B/msg, and a healthy run measures near zero.

Volume is part of the gate, not an accident of it. The run fails if fewer than
a million messages reached the window, because below that the per-message
number is noise and a pass would mean nothing. A saturated ten minutes clears
that by two orders of magnitude.

The first three minutes are discarded. Allocator arenas are bought once and
reused, so the opening minutes grow and then stop; judging from minute zero
reads that as a slow leak.

`/turbostats/v1` is polled for the whole run because it builds a bundle per
request. A handler that allocates per request leaks in proportion to requests,
which a sampler scraping once a minute would take hours to surface.

#### What a soak is not

A soak is a gate, not a diagnosis. When one fails, the leak is found with a
regression test that pushes message volume in seconds, not by staring at
another soak: see `internal/handlers/leak_test.go`, which drives half a
million messages through a handler in under a second. Add one of those for the
path you fixed, and keep the soak for the class nobody has written a test for.

## Writing

All prose follows Google's Technical Writing One. Active voice, one idea per
sentence, no hedging, the important thing first. Commit messages name the
defect, the fix, and the evidence, and say what breaks if the change is wrong.

Code comments explain why. The code already shows what.
