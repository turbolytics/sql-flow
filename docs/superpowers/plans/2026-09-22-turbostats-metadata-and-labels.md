# TurboStats Metadata and Labels Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A report says what the pipeline is made of, and carries the operator's own labels, so a fleet can be grouped and filtered by either.

**Architecture:** The config already names the source, sink and handler types. The run and serve commands copy them into the bundle's `instance` section, along with a bounded label map the operator declares in the `turbostats` block. `sqlflow validate` enforces the label rules, so a bad set fails before the pipeline starts rather than once a minute.

**Tech Stack:** Go 1.26, the repo's config validation (`internal/config`), `github.com/zeebo/assert`, `internal/coverage` markers.

**Spec:** `docs/superpowers/specs/2026-09-22-turbostats-v1-signals-design.md`, section "Metadata". This is PR 3 of that spec's four.

**Depends on:** nothing in PRs 1 and 2. It can be written in parallel and merged in any order; it touches `instance`, not `pipeline`.

## Global Constraints

- A process's field set is fixed from its first report to its last. Labels come from the config and nothing writes them at runtime.
- At most 10 labels. A key is at most 32 characters and matches `[a-z][a-z0-9_]*`. A value is at most 64 characters.
- These label names are refused: `id`, `name`, `version`, `commit`, `arch`, `config_hash`, `source_type`, `sink_type`, `handler_type`. A label that shadows a contract field makes two different things share a name.
- Validation happens at startup, in `sqlflow validate` and in the commands, never at report time.
- `source_type`, `sink_type` and `handler_type` are the config's own spellings, lowercased, so they match the documentation an operator reads.
- A serve process reports none of the three types: it has no source, no handler and no sink. Absent is not empty.
- The document stays `v1`. Every field is additive.
- Every new test carries `coverage.Covers(t, "observability.turbostats")`, or `"config.validation"` for the validation tests.
- Prose follows the repo's CLAUDE.md.

---

## File Structure

- Modify `turbostats/wire/bundle.go`: `Instance.SourceType`, `SinkType`, `HandlerType`, `Labels`.
- Modify `internal/config/turbostats.go`: the `Labels` field and its rules.
- Modify `internal/config/turbostats_test.go`.
- Modify `internal/turbostats/bundle.go`: `Static.SourceType`, `SinkType`, `HandlerType`, `Labels`.
- Modify `internal/turbostats/collect.go`: copy them into the instance section.
- Modify `internal/turbostats/collect_test.go`.
- Modify `internal/cli/run/root.go` and `internal/cli/serve/serve.go`: fill `Static` from the config.
- Modify `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md` and `CHANGELOG.md`.

---

### Task 1: The wire fields

**Files:**
- Modify: `turbostats/wire/bundle.go` (the `Instance` struct)
- Test: `turbostats/wire/bundle_test.go`

**Interfaces:**
- Produces: `Instance.SourceType, SinkType, HandlerType string` (omitempty), `Instance.Labels map[string]string` (omitempty).

- [ ] **Step 1: Write the failing test**

```go
// What the pipeline is made of, so "every stream that reads from Kafka" is
// a query. A serve process has none of the three, and sends none of them
// rather than sending empty strings.
func TestInstance_TypesAndLabels(t *testing.T) {
	with := mustMarshal(t, Bundle{V: Version, Instance: Instance{
		ID: "gw-1", SourceType: "kafka", SinkType: "postgres", HandlerType: "structured",
		Labels: map[string]string{"region": "eu-west"}}})
	for _, want := range []string{`"source_type":"kafka"`, `"sink_type":"postgres"`,
		`"handler_type":"structured"`, `"labels":{"region":"eu-west"}`} {
		if !strings.Contains(with, want) {
			t.Errorf("the instance is missing %s: %s", want, with)
		}
	}

	without := mustMarshal(t, Bundle{V: Version, Instance: Instance{ID: "gw-1"}})
	for _, absent := range []string{"source_type", "sink_type", "handler_type", "labels"} {
		if strings.Contains(without, absent) {
			t.Errorf("an instance with no %s carries one: %s", absent, without)
		}
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -short ./turbostats/wire/ -run TestInstance_TypesAndLabels`
Expected: FAIL, `unknown field SourceType`.

- [ ] **Step 3: Add the fields**

In `turbostats/wire/bundle.go`, in `Instance`, after `ConfigHash`:

```go
	// What this instance is made of, from its config. A receiver groups a
	// fleet by these: "every stream that reads from Kafka" is a query
	// rather than a grep. A serve process has none of them and sends none of
	// them.
	SourceType  string `json:"source_type,omitempty"`
	SinkType    string `json:"sink_type,omitempty"`
	HandlerType string `json:"handler_type,omitempty"`
	// Labels are the operator's own, declared in the config and fixed for
	// the life of the process. At most 10, keys [a-z][a-z0-9_]* up to 32
	// characters, values up to 64, and never a name this contract already
	// defines. The bounds are what keep a report a fixed shape and a
	// bounded size.
	Labels map[string]string `json:"labels,omitempty"`
```

- [ ] **Step 4: Run it to verify it passes**

Run: `go test -short ./turbostats/wire/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add turbostats/wire/
git commit -m "wire: what an instance is made of, and the operator's labels"
```

---

### Task 2: Labels in the config, with their rules

**Files:**
- Modify: `internal/config/turbostats.go`
- Test: `internal/config/turbostats_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `config.TurboStats.Labels map[string]string`, validated by the existing `Check(at []string) []Violation`.

- [ ] **Step 1: Write the failing tests**

```go
// The rules exist so a report keeps a fixed shape and a bounded size, and
// so a label never shadows a field the contract defines.
func TestTurboStatsLabels_RefusesWhatBreaksTheShape(t *testing.T) {
	coverage.Covers(t, "config.validation")
	tooMany := map[string]string{}
	for i := 0; i < 11; i++ {
		tooMany[fmt.Sprintf("k%d", i)] = "v"
	}
	cases := map[string]map[string]string{
		"too many":      tooMany,
		"reserved name": {"version": "v1"},
		"upper case":    {"Region": "eu"},
		"leading digit": {"1region": "eu"},
		"long key":      {strings.Repeat("k", 33): "eu"},
		"long value":    {"region": strings.Repeat("v", 65)},
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			ts := &TurboStats{ID: "gw-1", ReportTo: "https://c.example/v1/turbostats",
				Key: validCredentialForTest, Labels: labels}
			assert.That(t, len(ts.Check([]string{"pipeline", "turbostats"})) > 0)
		})
	}
}

// The ordinary case passes, and so does no labels at all.
func TestTurboStatsLabels_AcceptsABoundedSet(t *testing.T) {
	coverage.Covers(t, "config.validation")
	ts := &TurboStats{ID: "gw-1", ReportTo: "https://c.example/v1/turbostats",
		Key: validCredentialForTest,
		Labels: map[string]string{"region": "eu_west", "tenant": "acme", "env": "prod"}}
	assert.Equal(t, 0, len(ts.Check([]string{"pipeline", "turbostats"})))

	ts.Labels = nil
	assert.Equal(t, 0, len(ts.Check([]string{"pipeline", "turbostats"})))
}

// Labels are validated even with reporting off: a config that would be
// refused the moment someone sets report_to is a config with a defect in
// it now.
func TestTurboStatsLabels_AreCheckedWithReportingOff(t *testing.T) {
	coverage.Covers(t, "config.validation")
	ts := &TurboStats{Labels: map[string]string{"Region": "eu"}}
	assert.That(t, len(ts.Check([]string{"pipeline", "turbostats"})) > 0)
}
```

`validCredentialForTest` stands for the credential the file's other tests
already use, `sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8`. Write it out
or lift it to a constant; do not invent a second one.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -short ./internal/config/ -run TestTurboStatsLabels`
Expected: FAIL, `unknown field Labels`.

- [ ] **Step 3: Add the field and the rules**

In `internal/config/turbostats.go`, add `regexp` to the imports, then add to
`TurboStats`:

```go
	// Labels are the operator's own, copied into every report. They are
	// fixed for the life of the process: a receiver stores a series per
	// field, and a set that changed mid-run would split one instance's
	// history in two.
	Labels map[string]string `yaml:"labels,omitempty"`
```

Add the rules and the reserved set:

```go
// The limits on labels. A report has to stay a fixed shape and a bounded
// size: these hold on a cellular uplink at a report a minute, and they stop
// a loop over a data value from growing the bundle.
const (
	MaxLabels         = 10
	MaxLabelKeyLen    = 32
	MaxLabelValueLen  = 64
)

// labelKey is what a receiver can use as a column or a filter name without
// escaping it.
var labelKey = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// reservedLabels are the names this contract already defines. A label that
// shadows one makes two different things share a name.
var reservedLabels = map[string]bool{
	"id": true, "name": true, "version": true, "commit": true, "arch": true,
	"config_hash": true, "source_type": true, "sink_type": true, "handler_type": true,
}
```

In `Check`, before the `if !t.Enabled()` early return, so labels are checked whether or not reporting is on:

```go
	var out []Violation
	add := func(code errs.Code, key, format string, args ...any) {
		out = append(out, Violation{
			Code: code, Path: append(append([]string{}, at...), key),
			Message: fmt.Sprintf(format, args...),
		})
	}

	if len(t.Labels) > MaxLabels {
		add(errs.CodeConfigInvalid, "labels",
			"turbostats.labels has %d entries; at most %d", len(t.Labels), MaxLabels)
	}
	for k, v := range t.Labels {
		switch {
		case reservedLabels[k]:
			add(errs.CodeConfigInvalid, "labels",
				"turbostats.labels.%s is a field the bundle already carries", k)
		case !labelKey.MatchString(k):
			add(errs.CodeConfigInvalid, "labels",
				"turbostats.labels.%s is not a label name: lower case, starting with a letter, [a-z0-9_]", k)
		case len(k) > MaxLabelKeyLen:
			add(errs.CodeConfigInvalid, "labels",
				"turbostats.labels.%s is %d characters; at most %d", k, len(k), MaxLabelKeyLen)
		case len(v) > MaxLabelValueLen:
			add(errs.CodeConfigInvalid, "labels",
				"turbostats.labels.%s has a %d character value; at most %d", k, len(v), MaxLabelValueLen)
		}
	}
```

The existing body declares `out` and `add` after the early return. Move those two declarations above it, keep the early return as `if !t.Enabled() { return out }`, and leave the rest unchanged. Its comment says nothing to validate when reporting is off; update it to say labels are still checked, because a config that breaks the moment someone sets `report_to` has the defect now.

- [ ] **Step 4: Run the tests**

Run: `go test -short ./internal/config/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/
git commit -m "config: bounded labels on the turbostats block

Ten labels, lower-case keys, and never a name the bundle already carries.
Checked with reporting off too: a config that breaks the moment someone
sets report_to has the defect now."
```

---

### Task 3: The commands fill them in

**Files:**
- Modify: `internal/turbostats/bundle.go` (`Static`)
- Modify: `internal/turbostats/collect.go` (`Collect`'s instance literal)
- Modify: `internal/cli/run/root.go`, `internal/cli/serve/serve.go`
- Test: `internal/turbostats/collect_test.go`, `internal/cli/run/metrics_test.go`

**Interfaces:**
- Consumes: the wire fields from Task 1, `config.TurboStats.Labels` from Task 2.
- Produces: `Static.SourceType, SinkType, HandlerType string`, `Static.Labels map[string]string`.

- [ ] **Step 1: Write the failing test**

Append to `internal/turbostats/collect_test.go`:

```go
func TestCollect_CarriesTheTypesAndLabels(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	src := runSource(reader, nil)
	src.Static.SourceType = "kafka"
	src.Static.SinkType = "postgres"
	src.Static.HandlerType = "structured"
	src.Static.Labels = map[string]string{"region": "eu_west"}

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, "kafka", b.Instance.SourceType)
	assert.Equal(t, "postgres", b.Instance.SinkType)
	assert.Equal(t, "structured", b.Instance.HandlerType)
	assert.Equal(t, "eu_west", b.Instance.Labels["region"])
}

// The labels a process reports are the ones it started with. Collect copies
// the map so a caller that mutates its own cannot change a bundle already
// built, or a bundle being built on another goroutine.
func TestCollect_CopiesTheLabels(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	labels := map[string]string{"region": "eu_west"}
	src := runSource(reader, nil)
	src.Static.Labels = labels

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	labels["region"] = "us_east"
	assert.Equal(t, "eu_west", b.Instance.Labels["region"])
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -short ./internal/turbostats/ -run 'TestCollect_CarriesTheTypes|TestCollect_CopiesTheLabels'`
Expected: FAIL, `src.Static.SourceType` undefined.

- [ ] **Step 3: Add the fields and copy them**

In `internal/turbostats/bundle.go`, add to `Static`:

```go
	// What this process is made of, from its config. A serve process leaves
	// all three empty: it answers queries over datasets.
	SourceType  string
	SinkType    string
	HandlerType string
	// Labels are the operator's, already validated by the config.
	Labels map[string]string
```

In `internal/turbostats/collect.go`, in the `Instance` literal inside `Collect`:

```go
		Instance: Instance{
			ID:          s.ID,
			Name:        s.Name,
			Version:     s.Version,
			Commit:      s.Commit,
			Arch:        runtime.GOOS + "/" + runtime.GOARCH,
			ConfigHash:  s.ConfigHash,
			SourceType:  s.SourceType,
			SinkType:    s.SinkType,
			HandlerType: s.HandlerType,
			Labels:      copyLabels(s.Labels),
		},
```

and beside `Collect`:

```go
// copyLabels copies the operator's labels into the bundle.
//
// Collect promises to touch nothing shared, because the reporter and the
// HTTP handler call it at once. Handing out the same map would break that
// promise the first time anything wrote to it.
func copyLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
```

In `internal/cli/run/root.go`, where `turbostats.Static` is built, add:

```go
				SourceType:  strings.ToLower(conf.Pipeline.Source.Type),
				SinkType:    strings.ToLower(conf.Pipeline.Sink.Type),
				HandlerType: handlerTypeName(conf.Pipeline.Handler.Type),
				Labels:      ts.Labels,
```

`handlerTypeName` maps the config's handler type to the short name the contract uses. The config spells it `handlers.InferredMemBatch`; the bundle carries `inferred_mem`. Add the mapping beside the Static literal:

```go
// handlerTypeName is the bundle's short name for a handler. The config
// spells a Go type; a receiver groups a fleet by a word.
func handlerTypeName(configured string) string {
	switch configured {
	case "handlers.StructuredBatch":
		return "structured"
	case "handlers.InferredMemBatch":
		return "inferred_mem"
	case "handlers.InferredDiskBatch":
		return "inferred_disk"
	}
	return ""
}
```

Check the exact config spellings first with `grep -rn 'handlers\.' dev/config/examples | head`, and cover every one the examples use. An unknown handler reports no type rather than a guess.

In `internal/cli/serve/serve.go`, add `Labels: ts.Labels` and nothing else. A
serve config has no source, no handler and no sink: it answers queries over
datasets. All three types stay empty, and `omitempty` keeps them out of the
document.

- [ ] **Step 4: Run the tests**

Run: `go vet ./... && go test -short -race ./...`
Expected: PASS.

- [ ] **Step 5: Check a real bundle**

Run a pipeline with `--turbostats` and labels in its config, then:

```bash
curl -s localhost:8000/turbostats/v1 | python3 -c 'import sys,json; i=json.load(sys.stdin)["instance"]; print({k:i.get(k) for k in ("source_type","sink_type","handler_type","labels")})'
```

Expected: the types match the config, and the labels are the ones declared. Paste it into the PR.

- [ ] **Step 6: Commit**

```bash
git add internal/turbostats/ internal/cli/
git commit -m "turbostats: report what the pipeline is made of, and its labels

The config knows the source, sink and handler; a receiver groups a fleet
by them. Collect copies the label map, because it promises to touch
nothing shared and two callers read it at once."
```

---

### Task 4: The contract, the changelog, and the config example

**Files:**
- Modify: `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md`
- Modify: `CHANGELOG.md`
- Modify: one example under `dev/config/examples/` that already has a `turbostats` block, or the nearest one, to show labels

- [ ] **Step 1: Add the fields to the contract**

```markdown
### Metadata (added 2026-09-22)

`instance` carries what the process is made of, from its config:
`source_type`, `sink_type` and `handler_type`, lower case. A serve process
has none of the three and sends none of them.

`instance.labels` is the operator's own map, declared in the `turbostats`
block and fixed for the life of the process. At most 10 entries; a key
matches `[a-z][a-z0-9_]*` and is at most 32 characters; a value is at most
64. These names are refused, because a label that shadows a field this
contract defines makes two things share a name: `id`, `name`, `version`,
`commit`, `arch`, `config_hash`, `source_type`, `sink_type`, `handler_type`.
`sqlflow validate` enforces all of it, whether or not reporting is on.
```

- [ ] **Step 2: Add the changelog entry**

```markdown
- The TurboStats bundle says what a pipeline is made of: `source_type`,
  `sink_type` and `handler_type` in the `instance` section, so a fleet can
  be grouped by them. The `turbostats` block also takes `labels`, an
  operator's own key-value pairs, copied into every report: at most 10,
  lower-case keys up to 32 characters, values up to 64, and never a name
  the bundle already carries. `sqlflow validate` refuses a set that breaks
  those rules, with reporting on or off.
```

- [ ] **Step 3: Show labels in an example config**

Add to the `turbostats` block of one example:

```yaml
    labels:
      region: eu_west
      env: prod
```

Run `go test -short ./internal/cli/ -run Examples` afterwards: the examples are validated by a test, and this proves the block parses.

- [ ] **Step 4: Commit and open the PR**

```bash
git add docs/ CHANGELOG.md dev/config/examples/
git commit -m "docs: the instance's types and the operator's labels"
```

Run the gate:

```bash
go vet ./... && go test -short -race ./...
uv run --locked pytest tests/tooling -q
make coverage-page && git status --short docs/coverage
```

`make soak` is not required: nothing here runs per message or per batch. Say so in the PR. No session links, no attribution lines.
