# Webhook source: a listen address the config sets

No issue yet. Verified against `main` at bc86bc7 on 2026-09-18. Part of
[Deploy to Render](2026-09-18-render-metrics-template-design.md).

## The problem

The webhook source listens on `0.0.0.0:8001`, a constant in
`internal/webhook/source.go`. The README says "(not configurable)". The
constant dates from the Python engine, whose configs and proxies pointed at
that port.

A hosting platform picks the port. Render routes a web service to the port
in `PORT`, and a pipeline that cannot listen there cannot be deployed as one.
`sqlflow serve` already takes `serve.http.addr` from its config, and the webhook
source already has `WithAddr`, used only by tests.

## Scope

In:

- `source.webhook.addr`, a `host:port` string, optional.
- The README's webhook section and the config example golden file.

Out:

- TLS. The platform terminates it.
- A configurable path. `POST /events` stays.
- A health route. Added here only if the first Render deploy shows a web
  service needs one; the umbrella spec records that check.

## Decisions

| Decision | Choice | Rejected |
|---|---|---|
| Field | `addr`, a `host:port` string. | `port` alone. `serve.http.addr` is a `host:port`, and one term for one thing. |
| Default | `0.0.0.0:8001` when absent or empty. | No default. Every existing webhook config would break. |
| Env var | None in the engine. A config writes `"0.0.0.0:{{ PORT }}"`. | Reading `PORT` in Go. Configs are where sqlflow reads the environment. |

## The change

`internal/config/config.go`, on `WebhookSource`:

```go
// The address the listener binds, host:port. Defaults to 0.0.0.0:8001.
Addr string `yaml:"addr,omitempty"`
```

`internal/sources/init.go` appends `webhook.WithAddr(c.Webhook.Addr)` when
the field is non-empty, and adds the address to the "initializing webhook
source" log line.

Validation reuses `validAddr` from `internal/config/serve.go`, which
`serve.http.addr` already goes through, and reports the violation at
`pipeline.source.webhook.addr` in the same words: `%q is not a host:port`.
The tests below pin what `validAddr` accepts rather than restating it here.

An empty string means the default rather than an error. A template whose
variable is unset renders `addr: ""`, and the template's own
`|default('8001')` is the better place to catch that, but a bare empty value
must not bind a random port.

`internal/validate/schemas/config.json` and
`internal/cli/testdata/config_example.golden` regenerate.

The README's webhook section drops "(not configurable)" and shows the field.
`CHANGELOG.md` gains an entry.

## Testing

- `internal/config/webhook_test.go`: `addr` parses; absent and empty resolve
  to the default; `"nonsense"` and `"host"` are refused with the field's
  path; `":8001"` is accepted.
- `internal/sources/init_test.go`: a config with `addr: "127.0.0.1:0"` yields
  a source whose `Addr()` is on 127.0.0.1; a config without it binds 8001's
  default. The second case asserts the option list, not a bind, so the test
  does not need port 8001 free.
- The golden and schema tests pass after regeneration.
