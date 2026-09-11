<!--
The conventions are in CONTRIBUTING.md. Paste output, not adjectives: a
reviewer should be able to re-run every claim below.
-->

## What this changes

<!-- The defect or the gap, then the fix. What breaks if the change is wrong. -->

## Verification

<!-- Delete a row only if it genuinely does not apply, and say why. -->

- [ ] `go test -short -race ./...`
- [ ] `uv run --locked pytest tests/tooling -q`
- [ ] `make coverage-page && git status --short docs/coverage` is clean
- [ ] `uv run --locked pytest tests/release -q` — required when the CLI
      surface, a flag, the Dockerfile, or anything stamped at build time
      changed
- [ ] `make soak` — required when the change touches Arrow, ADBC, DuckDB,
      cgo, the consume loop, a handler, a sink, the metrics recording path,
      or anything that allocates per message or per request

### Soak verdict

<!--
Paste the block, not a summary. It carries the window, the message count and
the bytes per message, which is what makes it checkable.

window        minutes 3-10, 41,203,884 messages
native        3.9 -> 4.1 MiB
go retained   22.4 -> 22.6 MiB
native slope  +0.021 MiB/min
per message   +0.005 B/msg (threshold 1.0)

PASS  memory is flat over 41,203,884 messages
-->

```
```

## Notes for the reviewer

<!-- Anything you decided rather than derived, and any deviation from a spec. -->
