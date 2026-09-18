# Webhook Listen Address Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a pipeline config set the address the webhook source listens on, so a platform that assigns the port can run it.

**Architecture:** `WebhookSource` gains an optional `addr`. A `ResolvedAddr` method defaults and validates it, beside `ResolvedMaxBodyBytes` and `ResolvedMaxConnections`. The source registry passes the result to the existing `webhook.WithAddr` option. Nothing in `internal/webhook` changes.

**Tech Stack:** Go, `github.com/zeebo/assert`, the repo's `coverage.Covers` test registry, `make schema` for generated files.

**Spec:** `docs/superpowers/specs/2026-09-18-webhook-addr-design.md`

## Global Constraints

- Default address: `0.0.0.0:8001`, when `addr` is absent or empty.
- The engine reads no environment variable for the port. A config writes `"0.0.0.0:{{ PORT }}"`.
- Validation reuses `validAddr` in `internal/config/serve.go`. Do not write a second parser.
- Every test calls `coverage.Covers(t, "<feature id>")` first. Config tests use `config.templating`; source registry tests use `sink.retry`. Test names start with the matching prefix: `TestConfigTemplating_`, `TestSinkRetry_`.
- All prose follows `CLAUDE.md`: active voice, short sentences, comments explain why.
- Commit messages name the defect, the fix, and the evidence.
- Branch from `main`: `feat/webhook-addr`.

## File Structure

| File | Change |
|---|---|
| `internal/config/config.go` | `DefaultWebhookAddr`, `WebhookSource.Addr`, `ResolvedAddr`. |
| `internal/config/webhook_test.go` | `ResolvedAddr` cases. |
| `internal/sources/init.go` | Resolve the address, pass `webhook.WithAddr`, log it. |
| `internal/sources/init_test.go` | The registry honors `addr` and refuses a bad one. |
| `internal/webhook/source.go` | Comment on `defaultAddr` only. |
| `internal/validate/schemas/config.json` | Regenerated. |
| `internal/cli/testdata/config_example.golden` | Regenerated. |
| `README.md`, `CHANGELOG.md` | The field, and an Unreleased entry. |

---

### Task 1: `addr` in the config, defaulted and validated

**Files:**
- Modify: `internal/config/config.go` (the `WebhookSource` block, near line 319)
- Test: `internal/config/webhook_test.go`

**Interfaces:**
- Consumes: `validAddr(addr string) bool` from `internal/config/serve.go`, same package.
- Produces: `const DefaultWebhookAddr = "0.0.0.0:8001"`, field `WebhookSource.Addr string`, and `func (w *WebhookSource) ResolvedAddr() (string, error)`. Task 2 calls `ResolvedAddr`.

- [ ] **Step 1: Write the failing test**

Append to `internal/config/webhook_test.go`:

```go
// An absent block and an empty field both take the default. An empty field is
// what a template renders for an unset variable, and it must not reach
// net.Listen, which reads "" as every interface on a port the kernel picks.
func TestConfigTemplating_WebhookAddr(t *testing.T) {
	coverage.Covers(t, "config.templating")

	var absent *WebhookSource
	addr, err := absent.ResolvedAddr()
	assert.NoError(t, err)
	assert.Equal(t, DefaultWebhookAddr, addr)

	addr, err = (&WebhookSource{}).ResolvedAddr()
	assert.NoError(t, err)
	assert.Equal(t, DefaultWebhookAddr, addr)

	for _, ok := range []string{"0.0.0.0:10000", "127.0.0.1:0", ":8001"} {
		addr, err = (&WebhookSource{Addr: ok}).ResolvedAddr()
		assert.NoError(t, err)
		assert.Equal(t, ok, addr)
	}

	for _, bad := range []string{"nonsense", "host", "0.0.0.0:http", "0.0.0.0:70000"} {
		_, err = (&WebhookSource{Addr: bad}).ResolvedAddr()
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "addr"))
		assert.That(t, strings.Contains(err.Error(), bad))
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/config/ -run TestConfigTemplating_WebhookAddr`
Expected: build failure, `absent.ResolvedAddr undefined` and `undefined: DefaultWebhookAddr`.

- [ ] **Step 3: Implement**

In `internal/config/config.go`, add the constant after `DefaultWebhookMaxConnections`:

```go
// DefaultWebhookAddr is where the Python engine listened. Configs and reverse
// proxies written for it point at this port.
const DefaultWebhookAddr = "0.0.0.0:8001"
```

Add the field as the first field of `WebhookSource`:

```go
	// The address the listener binds, as host:port. Defaults to
	// 0.0.0.0:8001. A platform that assigns the port sets it from the
	// environment: "0.0.0.0:{{ PORT }}".
	Addr string `yaml:"addr,omitempty"`
```

Add the method after `ResolvedMaxBodyBytes`:

```go
// ResolvedAddr is the listen address in effect, defaulted. A nil receiver is
// the absent block. Empty means the default rather than an error: it is what
// a template renders for an unset variable, and net.Listen would read it as
// a port the kernel picks.
func (w *WebhookSource) ResolvedAddr() (string, error) {
	if w == nil || w.Addr == "" {
		return DefaultWebhookAddr, nil
	}
	if !validAddr(w.Addr) {
		return "", errs.New(errs.CodeSourceInvalid, "webhook source: addr %q is not a host:port", w.Addr)
	}
	return w.Addr, nil
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/config/ -run 'TestConfigTemplating_Webhook'`
Expected: PASS, three tests.

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/webhook_test.go
git commit -m "config: a webhook source declares the address it listens on

The webhook source binds 0.0.0.0:8001, a constant. A platform that
assigns the port, as Render does through PORT, cannot run it.

WebhookSource gains addr. ResolvedAddr defaults an absent or empty value
to the old constant and refuses anything validAddr refuses, the check
serve.http.addr already passes through. Empty is the default and not an
error because a template renders it for an unset variable, and
net.Listen reads it as a port the kernel picks."
```

---

### Task 2: the source registry binds the configured address

**Files:**
- Modify: `internal/sources/init.go` (the `"webhook"` entry, near line 100)
- Modify: `internal/webhook/source.go:22-24` (comment only)
- Test: `internal/sources/init_test.go`

**Interfaces:**
- Consumes: `(*config.WebhookSource).ResolvedAddr() (string, error)` from Task 1; `webhook.WithAddr(addr string) webhook.Option` and `(*webhook.Source).Addr() string`, both existing.
- Produces: nothing later tasks call.

- [ ] **Step 1: Write the failing test**

Append to `internal/sources/init_test.go`:

```go
// The address is the config's. Addr reports the configured address until the
// source starts, so neither case binds a port.
func TestSinkRetry_NewWebhookAddr(t *testing.T) {
	coverage.Covers(t, "sink.retry")
	s, err := New(config.Source{
		Type:    "webhook",
		Webhook: &config.WebhookSource{Addr: "127.0.0.1:10000"},
	}, zap.NewNop(), nil)
	assert.NoError(t, err)
	src := s.(*webhook.Source)
	assert.Equal(t, "127.0.0.1:10000", src.Addr())
	assert.NoError(t, src.Close())

	s, err = New(config.Source{
		Type:    "webhook",
		Webhook: &config.WebhookSource{},
	}, zap.NewNop(), nil)
	assert.NoError(t, err)
	src = s.(*webhook.Source)
	assert.Equal(t, config.DefaultWebhookAddr, src.Addr())
	assert.NoError(t, src.Close())

	_, err = New(config.Source{
		Type:    "webhook",
		Webhook: &config.WebhookSource{Addr: "nonsense"},
	}, zap.NewNop(), nil)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "addr"))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/sources/ -run TestSinkRetry_NewWebhookAddr`
Expected: FAIL. The first assertion reports `0.0.0.0:8001` where it wants `127.0.0.1:10000`.

- [ ] **Step 3: Implement**

In `internal/sources/init.go`, inside the `"webhook"` function, after the `maxConns` block and before the `l.Info("initializing webhook source"` call, add:

```go
		addr, err := c.Webhook.ResolvedAddr()
		if err != nil {
			return nil, err
		}
```

Add the address to that log call and to the options:

```go
		l.Info("initializing webhook source",
			zap.String("addr", addr),
			zap.Int64("max_body_bytes", maxBody),
			zap.Int("max_connections", maxConns),
		)
		opts := []webhook.Option{
			webhook.WithLogger(l),
			webhook.WithMeterProvider(mp),
			webhook.WithAddr(addr),
			webhook.WithMaxBodyBytes(maxBody),
			webhook.WithMaxConnections(maxConns),
		}
```

In `internal/webhook/source.go`, replace the comment above `defaultAddr` and the comment on `WithAddr`:

```go
// The Python engine served on 0.0.0.0:8001. It is the address a Source built
// without WithAddr binds; the config's default is config.DefaultWebhookAddr,
// the same value.
const defaultAddr = "0.0.0.0:8001"
```

```go
// WithAddr sets the listen address. The source registry passes the config's
// addr; tests pass port 0.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/sources/ ./internal/webhook/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sources/init.go internal/sources/init_test.go internal/webhook/source.go
git commit -m "webhook: bind the address the config declares

The source registry built every webhook source on the constant address,
so source.webhook.addr parsed and changed nothing.

The registry resolves addr, refuses a bad one at startup, passes it to
webhook.WithAddr, and logs it. WithAddr already existed for tests."
```

---

### Task 3: the generated schema and example, the README, and the changelog

**Files:**
- Regenerate: `internal/validate/schemas/config.json`, `internal/cli/testdata/config_example.golden`
- Modify: `README.md` (the `### Webhook` section, near line 757)
- Modify: `CHANGELOG.md` (under `## Unreleased`)

**Interfaces:**
- Consumes: the `Addr` field and its comment from Task 1. The schema's `description` and the example's comment are generated from that comment.
- Produces: nothing.

- [ ] **Step 1: Verify the committed files are stale**

Run: `go test ./internal/schema/ ./internal/cli/ -run 'TestConfigSchema_CommittedFileMatchesTheTypes|TestConfigValidation_ExampleMatchesPythonOutput'`
Expected: FAIL. Both committed files lack `addr`.

- [ ] **Step 2: Regenerate**

Run: `make schema`
Expected output ends with `regenerated internal/cli/testdata/config_example.golden and serve_example.golden`.

- [ ] **Step 3: Read the diff**

Run: `git diff --stat internal/validate/schemas internal/cli/testdata`
Expected: `config.json` and `config_example.golden` change. `serve.json`, `rollups.json` and `serve_example.golden` do not. If they do, stop: something other than this change regenerated.

Run: `git diff internal/cli/testdata/config_example.golden`
Expected: an `addr: <string>` line under `webhook:`, with the field's comment above it.

- [ ] **Step 4: Update the README**

In `README.md`, replace:

```markdown
Listens for `POST /events` on `0.0.0.0:8001` (not configurable) and optionally
validates an HMAC-SHA256 signature:
```

with:

```markdown
Listens for `POST /events` on `addr`, by default `0.0.0.0:8001`, and
optionally validates an HMAC-SHA256 signature:
```

In the YAML block that follows, add `addr` as the first key under `webhook:`:

```yaml
    addr: "0.0.0.0:8001" # optional, default 0.0.0.0:8001
```

After that YAML block, before "To send a signed event", add:

```markdown
A platform that assigns the port passes it through the template:
`addr: "0.0.0.0:{{ PORT }}"`. sqlflow refuses at startup an `addr` that is
not a `host:port`.
```

- [ ] **Step 5: Update the changelog**

In `CHANGELOG.md`, under `## Unreleased`, add an `### Added` section above `### Changed`:

```markdown
### Added

- `source.webhook.addr` sets the address the webhook source listens on. It
  was the constant `0.0.0.0:8001`, which is still the default. A platform
  that assigns the port, as Render does through `PORT`, could not run a
  webhook pipeline. An `addr` that is not a `host:port` fails at startup.
```

- [ ] **Step 6: Run the full unit pass**

Run: `make test-go`
Expected: PASS, including `gofmt` and `go vet`.

- [ ] **Step 7: Commit**

```bash
git add internal/validate/schemas/config.json internal/cli/testdata/config_example.golden README.md CHANGELOG.md
git commit -m "docs: source.webhook.addr in the schema, the example, and the README

The README said the webhook address was not configurable. It is now.
The schema and the example config are regenerated from the type, and
the changelog records the field."
```

---

## Verification

- [ ] `make test-go` passes.
- [ ] Run a pipeline on another port and send it a request:

```bash
cat > "$TMPDIR/addr.yml" <<'EOF'
pipeline:
  name: addr-check
  batch_size: 1
  source:
    type: webhook
    webhook:
      addr: "127.0.0.1:{{ PORT|default('18001') }}"
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT * FROM batch
  sink:
    type: console
EOF
go run ./cmd/sqlflow run -c "$TMPDIR/addr.yml" &
sleep 3
curl -s -X POST http://127.0.0.1:18001/events -d '{"a":1}'
kill %1
```

Expected: the log line `starting webhook server` carries `127.0.0.1:18001`, and `curl` prints `{"status":"received"}`. If `cmd/sqlflow` is not the binary's path, find it with `ls cmd/`.
