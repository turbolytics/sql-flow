# Static Validation, Step 1: template and schema checks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `sqlflow validate`, reporting every template and config-schema fault in one offline pass as JSON a machine can act on.

**Architecture:** A new `internal/validate` package exposes one entry point, `Validate(ctx, Request) (Report, error)`. It takes config **text**, never a path, and returns a serializable report of checks and diagnostics. Each check runs independently and appends to the report; nothing stops at the first failure. `internal/cli/validate.go` renders the report as text or JSON. SQL parse and bind checks are step 2 and are not in this plan.

**Tech Stack:** Go. Existing dependencies only: `github.com/nikolalohinski/gonja/v2` v2.9.0, `github.com/santhosh-tekuri/jsonschema/v6`, `gopkg.in/yaml.v3`, `github.com/spf13/cobra`, `github.com/zeebo/assert`.

**Spec:** `docs/superpowers/specs/2026-09-08-static-validation-design.md`

## Global Constraints

- **Offline.** No network, no broker, no sink. The config's `commands` block never executes in this step.
- **Every diagnostic in one pass.** No check stops at the first failure, and no check aborts the run.
- **A check that cannot run reports `skipped` with a reason.** It never reports `pass`. A false positive is worse than a miss, because a model obeys its linter and will edit correct config until the tool goes quiet.
- **Config travels as text.** `Request.Config` holds the content; `Request.Path` is for diagnostics only. A control-plane job shares no filesystem with its submitter (#178).
- **`Request` and `Report` survive a JSON round trip.** Enforced by test.
- **Prose style** follows `CLAUDE.md`: active voice, one idea per sentence, comments explain why.
- Error codes are append-only. Adding one is safe; regenerate the golden with `UPDATE_GOLDEN=1 go test ./internal/errs`.

## File Structure

| File | Responsibility |
|---|---|
| `internal/errs/registry.go` | Two new codes and their definitions (modify) |
| `internal/errs/testdata/codes.golden` | Regenerated (modify) |
| `internal/validate/report.go` | `Request`, `Report`, `Check`, `Diagnostic`, `Variables`, `Position` |
| `internal/validate/report_test.go` | JSON round trip |
| `internal/validate/template.go` | AST walk, variable sets, did-you-mean |
| `internal/validate/template_test.go` | Walk and set tests |
| `internal/validate/schema.go` | JSON Schema check, YAML line mapping |
| `internal/validate/schema_test.go` | Schema tests |
| `internal/validate/validate.go` | `Validate`, check orchestration |
| `internal/validate/validate_test.go` | Orchestration and the #120 regression |
| `internal/validate/testdata/issue-120.yml` | The reported config, typo intact |
| `internal/cli/validate.go` | `sqlflow validate`, `--json` |
| `internal/cli/root.go` | Register the command (modify) |
| `docs/coverage/features.yml` | Two feature rows (modify) |
| `docs/coverage/invariants.yml` | Three invariant rows (modify) |

---

### Task 1: Error codes and the report types

**Files:**
- Modify: `internal/errs/registry.go`
- Modify: `internal/errs/testdata/codes.golden`
- Create: `internal/validate/report.go`
- Test: `internal/validate/report_test.go`

**Interfaces:**
- Consumes: `errs.Code`, `errs.Definition`.
- Produces: `errs.CodeConfigTemplateUndefined`, `errs.CodeSQLParseFailed`, and the types `validate.Request`, `validate.Report`, `validate.Check`, `validate.Diagnostic`, `validate.Variables`, `validate.Position`, `validate.Status`, `validate.Severity`, plus `(*Report).Add`, `(*Report).SetCheck`, `(*Report).Finish`.

- [ ] **Step 1: Write the failing test**

Create `internal/validate/report_test.go`:

```go
package validate

import (
	"encoding/json"
	"testing"

	"github.com/zeebo/assert"
)

// The control plane is pull-based: a validate job crosses a queue, so both
// ends of the contract must survive JSON with nothing lost (#178).
func TestValidateReport_SurvivesJSONRoundTrip(t *testing.T) {
	req := Request{
		Path:   "pipeline.yml",
		Config: "pipeline:\n  name: x\n",
		Vars:   map[string]string{"SQLFLOW_TOPIC": "events"},
	}

	var gotReq Request
	encoded, err := json.Marshal(req)
	assert.NoError(t, err)
	assert.NoError(t, json.Unmarshal(encoded, &gotReq))
	assert.DeepEqual(t, req, gotReq)

	rep := Report{
		Config: "pipeline.yml",
		Checks: []Check{
			{ID: "config.template", Status: StatusFail},
			{ID: "sql.bind", Status: StatusSkipped, Reason: "no schema without a sample"},
		},
		Diagnostics: []Diagnostic{{
			Code:       "user.config.template_undefined",
			Class:      "user",
			Severity:   SeverityError,
			Message:    "template variable SQLFLOW_TOPI is not defined",
			Position:   &Position{Source: "config", Line: 6, Column: 15},
			DidYouMean: []string{"SQLFLOW_TOPIC"},
			Action:     "Define the variable, or correct the name.",
		}},
		Variables: &Variables{
			Referenced: []string{"SQLFLOW_TOPI"},
			Provided:   []string{"SQLFLOW_TOPIC"},
			Missing:    []string{"SQLFLOW_TOPI"},
			Unused:     []string{"SQLFLOW_TOPIC"},
		},
	}
	rep.Finish()
	assert.That(t, !rep.OK)

	var gotRep Report
	encoded, err = json.Marshal(rep)
	assert.NoError(t, err)
	assert.NoError(t, json.Unmarshal(encoded, &gotRep))
	assert.DeepEqual(t, rep, gotRep)
}

// A warning is advice. It must not fail the run, or every convention hint
// becomes a CI gate nobody can turn off.
func TestValidateReport_WarningKeepsOK(t *testing.T) {
	rep := Report{Diagnostics: []Diagnostic{{
		Code:     "user.config.invalid",
		Severity: SeverityWarning,
		Message:  "sqlcommand sink never reads sqlflow_sink_batch",
	}}}
	rep.Finish()
	assert.That(t, rep.OK)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/validate/ -run TestValidateReport -v`
Expected: FAIL — the package does not exist.

- [ ] **Step 3: Add the error codes**

In `internal/errs/registry.go`, add to the const block beside the other config codes:

```go
	// Config: the pipeline file itself.
	CodeConfigNotFound          Code = "user.config.not_found"
	CodeConfigParseFailed       Code = "user.config.parse_failed"
	CodeConfigInvalid           Code = "user.config.invalid"
	CodeConfigTemplateUndefined Code = "user.config.template_undefined"
```

and beside the other SQL codes:

```go
	CodeSQLParseFailed     Code = "user.sql.parse_failed"
```

Add their definitions to the `registry` map:

```go
	CodeConfigTemplateUndefined: {
		CodeConfigTemplateUndefined,
		"The config reads a template variable that is not defined. It renders as an empty string.",
		"Define the variable, or correct its name. `sqlflow config render` shows what the config becomes.",
	},
	CodeSQLParseFailed: {
		CodeSQLParseFailed,
		"A SQL statement in the config does not parse.",
		"Correct the statement. The position names the line.",
	},
```

- [ ] **Step 4: Regenerate the golden and confirm the taxonomy test passes**

Run: `UPDATE_GOLDEN=1 go test ./internal/errs`
Then: `go test ./internal/errs -v -run TestErrorTaxonomy`
Expected: PASS. `internal/errs/testdata/codes.golden` gains two lines.

- [ ] **Step 5: Write the report types**

Create `internal/validate/report.go`:

```go
// Package validate checks a pipeline config without running it.
//
// Every check is offline: no broker, no sink, no network, and nothing from
// the config's commands block executes. That is what lets CI, a pre-commit
// hook, and an agent loop all call it without consequences.
package validate

// Status is the outcome of one check.
type Status string

const (
	StatusPass    Status = "pass"
	StatusFail    Status = "fail"
	StatusSkipped Status = "skipped"
)

// Severity separates a fault that must block from advice that must not.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// Position points at the place in the user's own text a diagnostic refers to.
//
// Rendered marks a position measured against the rendered config rather than
// the file on disk. A template with control structures shifts line numbers,
// and reporting a shifted line as if it were the source line sends an editor
// -- human or model -- to the wrong place.
type Position struct {
	Source   string `json:"source"`
	Line     int    `json:"line"`
	Column   int    `json:"column"`
	Rendered bool   `json:"rendered,omitempty"`
}

// Check records that one class of validation ran, and what it concluded.
//
// A check that could not run reports StatusSkipped with a Reason, never
// StatusPass. A consumer must be able to tell "checked and fine" from "not
// checked", because a model treats an unqualified pass as proof.
type Check struct {
	ID     string `json:"id"`
	Status Status `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// Diagnostic is one fault, carrying everything needed to fix it without
// reading the source: the code, where it is, and what to do.
type Diagnostic struct {
	Code       string    `json:"code"`
	Class      string    `json:"class"`
	Severity   Severity  `json:"severity"`
	Message    string    `json:"message"`
	Position   *Position `json:"position,omitempty"`
	Context    string    `json:"context,omitempty"`
	DidYouMean []string  `json:"did_you_mean,omitempty"`
	Action     string    `json:"action,omitempty"`
}

// Variables reports the template's variable use from both sides.
//
// Unused is what makes a typo name itself: a missing SQLFLOW_AZURE_CONNECTION
// alongside an unread SQLFLOW_AZURE_STORAGE_CONNECTION is a misspelling, and
// reporting only the missing half leaves the reader to guess (#120).
type Variables struct {
	Referenced []string `json:"referenced"`
	Provided   []string `json:"provided"`
	Missing    []string `json:"missing"`
	Unused     []string `json:"unused,omitempty"`
}

// Request is one validation job.
//
// Config carries the config's text. Path names the file for diagnostics only.
// The split is deliberate: the pull-based control plane hands a job to an
// instance that shares no filesystem with the submitter (#178).
type Request struct {
	Path   string            `json:"path,omitempty"`
	Config string            `json:"config"`
	Vars   map[string]string `json:"vars,omitempty"`
}

// Report is the result of a validation job.
type Report struct {
	Config      string       `json:"config,omitempty"`
	OK          bool         `json:"ok"`
	Checks      []Check      `json:"checks"`
	Diagnostics []Diagnostic `json:"diagnostics"`
	Variables   *Variables   `json:"variables,omitempty"`
}

// Add appends a diagnostic.
func (r *Report) Add(d Diagnostic) {
	r.Diagnostics = append(r.Diagnostics, d)
}

// SetCheck records a check's outcome, replacing any earlier entry for the
// same id so a check cannot appear twice.
func (r *Report) SetCheck(id string, status Status, reason string) {
	for i := range r.Checks {
		if r.Checks[i].ID == id {
			r.Checks[i] = Check{ID: id, Status: status, Reason: reason}
			return
		}
	}
	r.Checks = append(r.Checks, Check{ID: id, Status: status, Reason: reason})
}

// Finish computes OK. Only an error clears it: a warning is advice, and
// gating CI on advice makes the advice unwelcome.
func (r *Report) Finish() {
	r.OK = true
	for _, d := range r.Diagnostics {
		if d.Severity == SeverityError {
			r.OK = false
			return
		}
	}
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./internal/validate/ ./internal/errs/ -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/errs/registry.go internal/errs/testdata/codes.golden internal/validate/report.go internal/validate/report_test.go
git commit -m "validate: the report contract, and two codes for it"
```

---

### Task 2: Collect every template variable reference

**Files:**
- Create: `internal/validate/template.go`
- Test: `internal/validate/template_test.go`

**Interfaces:**
- Consumes: `gonja.FromString`, `nodes.Template.Nodes`, and the exported expression node types.
- Produces: `type ref struct { name string; line, col int }` and `func scanTemplate(src string) (refs []ref, complete bool, err error)`.

**Why a hand-written walk:** `nodes.Inspect` stops at the first `*nodes.Data` node in gonja v2.9.0 and never reaches an expression. Verified 2026-09-08. The walk below uses the exported node fields instead.

- [ ] **Step 1: Write the failing test**

Create `internal/validate/template_test.go`:

```go
package validate

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestValidateTemplate_CollectsReferencesWithPositions(t *testing.T) {
	src := "brokers: [{{ SQLFLOW_KAFKA_BROKERS|default('localhost:9092') }}]\n" +
		"topic: \"{{ SQLFLOW_TOPIC }}\"\n"

	refs, complete, err := scanTemplate(src)
	assert.NoError(t, err)
	assert.That(t, complete)
	assert.Equal(t, 2, len(refs))

	assert.Equal(t, "SQLFLOW_KAFKA_BROKERS", refs[0].name)
	assert.Equal(t, 1, refs[0].line)
	assert.Equal(t, "SQLFLOW_TOPIC", refs[1].name)
	assert.Equal(t, 2, refs[1].line)
}

// A filter's arguments can themselves be variables, and missing one is the
// same fault as missing a bare reference.
func TestValidateTemplate_CollectsFilterArguments(t *testing.T) {
	refs, complete, err := scanTemplate("x: {{ A|default(B) }}\n")
	assert.NoError(t, err)
	assert.That(t, complete)

	names := refNames(refs)
	assert.That(t, contains(names, "A"))
	assert.That(t, contains(names, "B"))
}

// A control structure binds its own names and hides its body from this walk.
// Reporting incomplete is what stops the unused set from lying.
func TestValidateTemplate_ControlStructureMarksIncomplete(t *testing.T) {
	src := "{% for t in SQLFLOW_TOPICS %}\n- {{ t }}\n{% endfor %}\n"

	_, complete, err := scanTemplate(src)
	assert.NoError(t, err)
	assert.That(t, !complete)
}

func TestValidateTemplate_ParseErrorIsReturned(t *testing.T) {
	_, _, err := scanTemplate("x: {{ unclosed \n")
	assert.Error(t, err)
}

func refNames(refs []ref) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.name)
	}
	return out
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/validate/ -run TestValidateTemplate -v`
Expected: FAIL with `undefined: scanTemplate`.

- [ ] **Step 3: Write the walk**

Create `internal/validate/template.go`:

```go
package validate

import (
	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/nodes"
)

// ref is one variable reference, with the position of the name itself.
type ref struct {
	name string
	line int
	col  int
}

// scanTemplate parses the config as a Jinja2 template and collects every
// variable it reads.
//
// complete reports whether the walk understood every node it met. A control
// structure hides its body and binds its own loop names, and an unmodelled
// expression type could hide a reference. Either sets complete false, which
// suppresses the unused report: a name this walk never saw is not evidence
// that the config does not use it.
func scanTemplate(src string) (refs []ref, complete bool, err error) {
	tmpl, err := gonja.FromString(src)
	if err != nil {
		return nil, false, err
	}

	w := walker{complete: true}
	for _, n := range tmpl.Root().Nodes {
		w.walk(n)
	}
	return w.refs, w.complete, nil
}

type walker struct {
	refs     []ref
	complete bool
}

func (w *walker) walk(n nodes.Node) {
	if n == nil {
		return
	}

	switch v := n.(type) {
	case *nodes.Data, *nodes.Comment, *nodes.String, *nodes.Integer,
		*nodes.Float, *nodes.Bool, *nodes.None:
		// Literals read nothing.

	case *nodes.Output:
		w.walk(v.Expression)
		w.walk(v.Condition)
		w.walk(v.Alternative)

	case *nodes.FilteredExpression:
		w.walk(v.Expression)
		for _, f := range v.Filters {
			for _, a := range f.Args {
				w.walk(a)
			}
			for _, a := range f.Kwargs {
				w.walk(a)
			}
		}

	case *nodes.TestExpression:
		w.walk(v.Expression)

	case *nodes.Name:
		w.refs = append(w.refs, ref{v.Name.Val, v.Name.Line, v.Name.Col})

	case *nodes.Variable:
		// Only the root of a dotted lookup is a variable. The rest are
		// attributes of whatever it resolves to.
		if len(v.Parts) > 0 {
			w.refs = append(w.refs, ref{v.Parts[0].S, v.Location.Line, v.Location.Col})
		}

	case *nodes.GetAttribute:
		w.walk(v.Node)

	case *nodes.GetItem:
		w.walk(v.Node)
		w.walk(v.Arg)

	case *nodes.Call:
		w.walk(v.Func)
		for _, a := range v.Args {
			w.walk(a)
		}
		for _, a := range v.Kwargs {
			w.walk(a)
		}

	case *nodes.BinaryExpression:
		w.walk(v.Left)
		w.walk(v.Right)

	case *nodes.UnaryExpression:
		w.walk(v.Term)

	case *nodes.Negation:
		w.walk(v.Term)

	case *nodes.List:
		for _, e := range v.Val {
			w.walk(e)
		}

	case *nodes.Tuple:
		for _, e := range v.Val {
			w.walk(e)
		}

	case *nodes.Dict:
		for _, p := range v.Pairs {
			w.walk(p.Key)
			w.walk(p.Value)
		}

	default:
		// A node this walk does not model, a control structure above all.
		// Recording the gap is the honest response; guessing produces a
		// false unused report, and a model acts on those.
		w.complete = false
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/validate/ -run TestValidateTemplate -v`
Expected: PASS, all four.

Field names above are confirmed against gonja v2.9.0 on 2026-09-08: `Output.{Expression,Condition,Alternative}`, `FilteredExpression.{Expression,Filters}`, `FilterCall.{Name,Args,Kwargs}`, `Name.Name`, `Variable.{Location,Parts}`, `VariablePart.S`, `GetAttribute.Node`, `GetItem.{Node,Arg}`, `Call.{Func,Args,Kwargs}`, `BinaryExpression.{Left,Right}`, `UnaryExpression.Term`, `Negation.Term`, `List.Val`, `Tuple.Val`, `Dict.Pairs`, `Pair.{Key,Value}`.

- [ ] **Step 5: Commit**

```bash
git add internal/validate/template.go internal/validate/template_test.go
git commit -m "validate: collect every template variable, and admit what the walk missed"
```

---

### Task 3: Variable sets and did-you-mean

**Files:**
- Modify: `internal/validate/template.go`
- Test: `internal/validate/template_test.go`

**Interfaces:**
- Consumes: `scanTemplate`, `Report`, `Diagnostic`, `Variables`, `errs.CodeConfigTemplateUndefined`, `errs.Definition`.
- Produces: `func checkTemplate(src string, provided map[string]string, rep *Report)`.

- [ ] **Step 1: Write the failing test**

Append to `internal/validate/template_test.go`:

```go
// Issue #120: the reporter provided SQLFLOW_AZURE_STORAGE_CONNECTION_STRING
// and the config read SQLFLOW_AZURE_CONNECTION_STRING. gonja rendered the
// missing name as an empty string with a nil error, and the resulting
// authentication failure named nothing. Verified 2026-09-08.
func TestValidateTemplate_MissingVariableNamesItsNeighbour(t *testing.T) {
	src := "sql: SET conn = '{{ SQLFLOW_AZURE_CONNECTION_STRING }}';\n"
	provided := map[string]string{
		"SQLFLOW_AZURE_STORAGE_CONNECTION_STRING": "real",
	}

	var rep Report
	checkTemplate(src, provided, &rep)
	rep.Finish()

	assert.That(t, !rep.OK)
	assert.Equal(t, 1, len(rep.Diagnostics))

	d := rep.Diagnostics[0]
	assert.Equal(t, "user.config.template_undefined", d.Code)
	assert.Equal(t, SeverityError, d.Severity)
	assert.Equal(t, 1, d.Position.Line)
	assert.DeepEqual(t, []string{"SQLFLOW_AZURE_STORAGE_CONNECTION_STRING"}, d.DidYouMean)
	assert.That(t, d.Action != "")

	assert.DeepEqual(t, []string{"SQLFLOW_AZURE_CONNECTION_STRING"}, rep.Variables.Missing)
	assert.DeepEqual(t, []string{"SQLFLOW_AZURE_STORAGE_CONNECTION_STRING"}, rep.Variables.Unused)
}

func TestValidateTemplate_AllVariablesDefinedPasses(t *testing.T) {
	var rep Report
	checkTemplate("topic: {{ SQLFLOW_TOPIC }}\n",
		map[string]string{"SQLFLOW_TOPIC": "events"}, &rep)
	rep.Finish()

	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "config.template"))
	assert.Equal(t, 0, len(rep.Variables.Unused))
}

// An incomplete walk cannot prove a variable is unread, so it must not claim
// one is. The missing set stays trustworthy either way: a name the walk found
// really is referenced.
func TestValidateTemplate_IncompleteWalkSuppressesUnused(t *testing.T) {
	src := "{% for t in SQLFLOW_TOPICS %}\n- {{ t }}\n{% endfor %}\n"

	var rep Report
	checkTemplate(src, map[string]string{"SQLFLOW_UNREAD": "x"}, &rep)

	assert.Equal(t, 0, len(rep.Variables.Unused))
	assert.Equal(t, StatusSkipped, checkStatus(t, rep, "config.template.unused"))
}

// Every diagnostic in one pass. Two missing variables must not cost two runs.
func TestValidateTemplate_ReportsEveryMissingVariable(t *testing.T) {
	var rep Report
	checkTemplate("a: {{ ONE }}\nb: {{ TWO }}\n", nil, &rep)

	assert.Equal(t, 2, len(rep.Diagnostics))
}

func checkStatus(t *testing.T, rep Report, id string) Status {
	t.Helper()
	for _, c := range rep.Checks {
		if c.ID == id {
			return c.Status
		}
	}
	t.Fatalf("no check %q in %v", id, rep.Checks)
	return ""
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/validate/ -run TestValidateTemplate -v`
Expected: FAIL with `undefined: checkTemplate`.

- [ ] **Step 3: Implement the check**

Append to `internal/validate/template.go`:

```go
import (
	"sort"
	"strings"

	"github.com/turbolytics/sql-flow/internal/errs"
)

// checkTemplate reports the config's template faults into rep.
//
// It never returns early. A config with three missing variables reports three
// diagnostics in one run, because every round trip costs an agent a turn.
func checkTemplate(src string, provided map[string]string, rep *Report) {
	refs, complete, err := scanTemplate(src)
	if err != nil {
		rep.SetCheck("config.template", StatusFail, "")
		rep.Add(diagnostic(errs.CodeConfigParseFailed, SeverityError,
			"the config is not a valid template: "+err.Error(), nil))
		rep.Variables = &Variables{
			Referenced: []string{},
			Provided:   sortedKeys(provided),
			Missing:    []string{},
		}
		return
	}

	referenced := map[string]bool{}
	var missing []string
	seen := map[string]bool{}

	for _, r := range refs {
		referenced[r.name] = true
		if _, ok := provided[r.name]; ok {
			continue
		}
		// One diagnostic per name, at its first use. Repeating a variable
		// three times is one mistake, not three.
		if seen[r.name] {
			continue
		}
		seen[r.name] = true
		missing = append(missing, r.name)

		rep.Add(diagnostic(
			errs.CodeConfigTemplateUndefined,
			SeverityError,
			"template variable "+r.name+" is not defined and renders as an empty string",
			&Position{Source: "config", Line: r.line, Column: r.col},
			nearest(r.name, provided)...,
		))
	}

	sort.Strings(missing)
	if missing == nil {
		missing = []string{}
	}

	vars := &Variables{
		Referenced: sortedSet(referenced),
		Provided:   sortedKeys(provided),
		Missing:    missing,
	}

	// Only a complete walk can prove a variable goes unread.
	if complete {
		for name := range provided {
			if !referenced[name] {
				vars.Unused = append(vars.Unused, name)
			}
		}
		sort.Strings(vars.Unused)
		rep.SetCheck("config.template.unused", StatusPass, "")
	} else {
		rep.SetCheck("config.template.unused", StatusSkipped,
			"the template has control structures, so an unread variable cannot be proven")
	}

	rep.Variables = vars

	if len(missing) > 0 {
		rep.SetCheck("config.template", StatusFail, "")
	} else {
		rep.SetCheck("config.template", StatusPass, "")
	}
}

// nearest returns the provided names within edit distance 3 of want, closest
// first. It is what turns "missing SQLFLOW_AZURE_CONNECTION_STRING" into a
// pointer at the SQLFLOW_AZURE_STORAGE_CONNECTION_STRING sitting unread
// beside it.
func nearest(want string, provided map[string]string) []string {
	type scored struct {
		name string
		dist int
	}
	var candidates []scored
	for name := range provided {
		d := editDistance(strings.ToUpper(want), strings.ToUpper(name))
		// Scaled to the name's length: SQLFLOW_ prefixes make every candidate
		// long, and a fixed threshold either matches everything or nothing.
		limit := len(want)/4 + 1
		if d <= limit {
			candidates = append(candidates, scored{name, d})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].dist != candidates[j].dist {
			return candidates[i].dist < candidates[j].dist
		}
		return candidates[i].name < candidates[j].name
	})

	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.name)
	}
	return out
}

func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, min(cur[j-1]+1, prev[j-1]+cost))
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

// diagnostic builds a Diagnostic, taking Class and Action from the code's
// registry entry so the two cannot drift.
func diagnostic(code errs.Code, sev Severity, msg string, pos *Position, didYouMean ...string) Diagnostic {
	d := Diagnostic{
		Code:       string(code),
		Class:      string(code.Class()),
		Severity:   sev,
		Message:    msg,
		Position:   pos,
		DidYouMean: didYouMean,
	}
	if def, ok := errs.Lookup(code); ok {
		d.Action = def.Action
	}
	return d
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/validate/ -v`
Expected: PASS.

`errs.Lookup(Code) (Definition, bool)` already exists — confirmed 2026-09-08 — so this task changes nothing under `internal/errs`.

- [ ] **Step 5: Commit**

```bash
git add internal/validate/template.go internal/validate/template_test.go
git commit -m "validate: a missing template variable names the one sitting unread beside it"
```

---

### Task 4: The config schema check, with real line numbers

**Files:**
- Create: `internal/validate/schema.go`
- Test: `internal/validate/schema_test.go`

**Interfaces:**
- Consumes: `jsonschema.ValidationError`, `yaml.Node`, `Report`.
- Produces: `func checkSchema(rendered []byte, rep *Report)` and `func lineOf(root *yaml.Node, path []string) (line, col int, ok bool)`.

The embedded schema lives in `internal/cli/schemas/config.json` and `go:embed` cannot reach outside its own package. Move the embed into `internal/validate` and have `internal/cli` use the new package, keeping `TestEmbeddedSchemaMatchesPython` pointed at the file's new home.

- [ ] **Step 1: Write the failing test**

Create `internal/validate/schema_test.go`:

```go
package validate

import (
	"testing"

	"github.com/zeebo/assert"
)

// Issue #120's first fault: commands written as a mapping, not a sequence.
func TestValidateSchema_CommandsMustBeASequence(t *testing.T) {
	rendered := []byte(`commands:
  name: load extensions
  sql: INSTALL azure;
pipeline:
  source:
    type: kafka
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: console
`)

	var rep Report
	checkSchema(rendered, &rep)
	rep.Finish()

	assert.That(t, !rep.OK)
	assert.That(t, len(rep.Diagnostics) > 0)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "config.schema"))

	// The position must name the offending key, not the top of the file.
	assert.That(t, rep.Diagnostics[0].Position != nil)
	assert.Equal(t, 1, rep.Diagnostics[0].Position.Line)
}

func TestValidateSchema_ValidConfigPasses(t *testing.T) {
	rendered := []byte(`pipeline:
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: console
`)

	var rep Report
	checkSchema(rendered, &rep)
	rep.Finish()

	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "config.schema"))
}

func TestValidateSchema_MalformedYAMLIsOneDiagnostic(t *testing.T) {
	var rep Report
	checkSchema([]byte("pipeline:\n  - : :\n"), &rep)
	rep.Finish()

	assert.That(t, !rep.OK)
	assert.Equal(t, 1, len(rep.Diagnostics))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/validate/ -run TestValidateSchema -v`
Expected: FAIL with `undefined: checkSchema`.

- [ ] **Step 3: Move the schema and implement the check**

Run: `git mv internal/cli/schemas internal/validate/schemas`

Create `internal/validate/schema.go`:

```go
package validate

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// configSchemaJSON is the config JSON Schema. It moved here from
// internal/cli because go:embed cannot reach outside its own package and
// this package is now the one that validates against it.
//
//go:embed schemas/config.json
var configSchemaJSON []byte

const schemaURL = "https://turbolytics.io/schemas/config.json"

// checkSchema validates the rendered config against the config schema and
// reports every violation, each anchored to the line it came from.
func checkSchema(rendered []byte, rep *Report) {
	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck("config.schema", StatusFail, "")
		rep.Add(diagnostic(errs.CodeConfigParseFailed, SeverityError,
			"the config is not valid YAML: "+err.Error(), nil))
		return
	}

	var doc any
	if err := yaml.Unmarshal(rendered, &doc); err != nil {
		rep.SetCheck("config.schema", StatusFail, "")
		rep.Add(diagnostic(errs.CodeConfigParseFailed, SeverityError,
			"the config is not valid YAML: "+err.Error(), nil))
		return
	}

	// The validator works on the JSON data model, so the document is
	// round-tripped to normalize YAML's own types.
	normalized, err := jsonRoundTrip(doc)
	if err != nil {
		rep.SetCheck("config.schema", StatusFail, "")
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityError,
			"the config could not be normalized: "+err.Error(), nil))
		return
	}

	schema, err := compileSchema()
	if err != nil {
		// Ours, not the user's. Skipped rather than failed: the config was
		// never actually checked and must not be reported as sound.
		rep.SetCheck("config.schema", StatusSkipped, "the embedded schema failed to compile: "+err.Error())
		return
	}

	if err := schema.Validate(normalized); err != nil {
		rep.SetCheck("config.schema", StatusFail, "")
		var verr *jsonschema.ValidationError
		if !errors.As(err, &verr) {
			rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityError, err.Error(), nil))
			return
		}
		for _, leaf := range leaves(verr) {
			pos := (*Position)(nil)
			if line, col, ok := lineOf(&root, leaf.InstanceLocation); ok {
				pos = &Position{Source: "config", Line: line, Column: col}
			}
			// leaf.Error() rather than ErrorKind.LocalizedString: the latter
			// wants a *message.Printer, and a nil one is not documented as
			// safe. Error() already nests the cause under its location.
			d := diagnostic(errs.CodeConfigInvalid, SeverityError, leaf.Error(), pos)
			d.Context = "/" + joinPath(leaf.InstanceLocation)
			rep.Add(d)
		}
		return
	}

	rep.SetCheck("config.schema", StatusPass, "")
}

// leaves flattens a validation error to its most specific causes. The root
// error says only "the document is invalid"; the leaves name the keys.
func leaves(e *jsonschema.ValidationError) []*jsonschema.ValidationError {
	if len(e.Causes) == 0 {
		return []*jsonschema.ValidationError{e}
	}
	var out []*jsonschema.ValidationError
	for _, c := range e.Causes {
		out = append(out, leaves(c)...)
	}
	return out
}

// lineOf resolves a JSON-pointer-style instance path to a position in the
// YAML document. A diagnostic that cannot name a line sends its reader to the
// top of the file, which for a model means editing the wrong thing.
func lineOf(root *yaml.Node, path []string) (int, int, bool) {
	node := root
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		node = node.Content[0]
	}

	for _, seg := range path {
		switch node.Kind {
		case yaml.MappingNode:
			found := false
			// Content alternates key, value.
			for i := 0; i+1 < len(node.Content); i += 2 {
				if node.Content[i].Value == seg {
					// The key carries the position a reader wants, not the
					// value: an editor jumps to the name that is wrong.
					if i+2 >= len(node.Content) {
						return node.Content[i].Line, node.Content[i].Column, true
					}
					node = node.Content[i+1]
					found = true
					break
				}
			}
			if !found {
				return node.Line, node.Column, true
			}
		case yaml.SequenceNode:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node.Content) {
				return node.Line, node.Column, true
			}
			node = node.Content[idx]
		default:
			return node.Line, node.Column, true
		}
	}
	return node.Line, node.Column, true
}

func joinPath(path []string) string {
	out := ""
	for i, s := range path {
		if i > 0 {
			out += "/"
		}
		out += s
	}
	return out
}

func compileSchema() (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(configSchemaJSON))
	if err != nil {
		return nil, fmt.Errorf("parsing config schema failed: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(schemaURL, doc); err != nil {
		return nil, fmt.Errorf("loading config schema failed: %w", err)
	}
	return compiler.Compile(schemaURL)
}

func jsonRoundTrip(v any) (any, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, err
	}
	return out, nil
}
```

`jsonschema.ValidationError` carries `SchemaURL`, `InstanceLocation []string`, `ErrorKind`, and `Causes []*ValidationError` — confirmed against the pinned v6 on 2026-09-08. The root error says only that the document is invalid, which is why `leaves` walks to the causes that name keys.

- [ ] **Step 4: Point `internal/cli` at the moved schema**

In `internal/cli/config.go`, delete the `//go:embed schemas/config.json` block and `configSchemaJSON`, `compileConfigSchema`, and `jsonRoundTrip`. Change `validateConfig` to call `validate.Validate`, and keep `configExample` working by exporting the schema bytes from the new package:

```go
// in internal/validate/schema.go
// SchemaJSON is the embedded config schema. `config example` renders it as a
// commented skeleton.
func SchemaJSON() []byte { return configSchemaJSON }
```

Update `internal/cli/config.go`'s `configExample` to call `validate.SchemaJSON()`, and update any test referencing `internal/cli/schemas/config.json` to the new path.

- [ ] **Step 5: Run the full suite**

Run: `go test ./internal/validate/ ./internal/cli/ -v`
Expected: PASS, including the existing `TestEmbeddedSchemaMatchesPython`.

- [ ] **Step 6: Commit**

```bash
git add internal/validate/ internal/cli/
git commit -m "validate: schema violations name the key and the line"
```

---

### Task 5: Validate, running every check in one pass

**Files:**
- Create: `internal/validate/validate.go`
- Test: `internal/validate/validate_test.go`
- Create: `internal/validate/testdata/issue-120.yml`

**Interfaces:**
- Consumes: `checkTemplate`, `checkSchema`, `config.RenderTemplateString`.
- Produces: `func Validate(ctx context.Context, req Request) (Report, error)` and `func RenderTemplateString(src []byte, overrides map[string]string) ([]byte, error)` in `internal/config`.

- [ ] **Step 1: Add the text-based renderer**

`config.RenderTemplate` takes a path, and a validate job carries text. In `internal/config/load.go`, extract the body:

```go
// RenderTemplateString renders config text through Jinja2. RenderTemplate is
// the same thing for a file, and calls this.
//
// The text form exists because a validate job carries the config's content:
// the pull-based control plane hands a job to an instance that shares no
// filesystem with the submitter (#178).
func RenderTemplateString(src []byte, overrides map[string]string) ([]byte, error) {
	tmpl, err := gonja.FromBytes(src)
	if err != nil {
		return nil, errs.Wrap(errs.CodeConfigParseFailed, err, "parsing template failed")
	}

	vars := settingsVars()
	for _, v := range os.Environ() {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 && strings.HasPrefix(parts[0], "SQLFLOW_") {
			vars[parts[0]] = parts[1]
		}
	}
	for k, v := range overrides {
		vars[k] = v
	}

	out, err := tmpl.ExecuteToBytes(exec.NewContext(vars))
	if err != nil {
		return nil, errs.Wrap(errs.CodeConfigParseFailed, err, "rendering template failed")
	}
	return out, nil
}

// TemplateVars returns the context RenderTemplateString builds, so validate
// can report what the config had available to it.
func TemplateVars(overrides map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range settingsVars() {
		out[k] = fmt.Sprint(v)
	}
	for _, v := range os.Environ() {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 && strings.HasPrefix(parts[0], "SQLFLOW_") {
			out[parts[0]] = parts[1]
		}
	}
	for k, v := range overrides {
		out[k] = v
	}
	return out
}
```

`internal/config/load.go` does not import `fmt` today; `TemplateVars` needs it. `settingsVars()` returns `map[string]any` whose values are all strings, so `fmt.Sprint` is safe.

Rewrite `RenderTemplate` to read the file, keep its `CodeConfigNotFound` check, and delegate:

```go
func RenderTemplate(path string, overrides map[string]string) ([]byte, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, errs.New(errs.CodeConfigNotFound, "config file not found: %s", path)
	}
	return RenderTemplateString(src, overrides)
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/validate/testdata/issue-120.yml` — the reported config, faults intact:

```yaml
commands:
  - name: load extensions
    sql: |
      INSTALL azure;
      LOAD azure;
  - name: set azure storage connection string
    sql: |
      SET azure_storage_connection_string = '{{ SQLFLOW_AZURE_CONNECTION_STRING }}';

pipeline:
  name: kafka-azure-duckdb-sink
  batch_size: 50
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: sql-flow-consumer-001
      auto_offset_reset: earliest
      topics:
        - "events"
  handler:
    type: 'handlers.InferredMemBatch'
    sql: |
      SELECT * FROM batch
  sink:
    type: console
```

Create `internal/validate/validate_test.go`:

```go
package validate

import (
	"context"
	"os"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The regression this package exists for. The reporter in #120 provided
// SQLFLOW_AZURE_STORAGE_CONNECTION_STRING and read
// SQLFLOW_AZURE_CONNECTION_STRING. Nothing said so, and the failure surfaced
// as an authentication error days later.
func TestValidateNoSideEffects_Issue120TypoIsNamed(t *testing.T) {
	coverage.Covers(t, "validate.template", "validate.schema")

	src, err := os.ReadFile("testdata/issue-120.yml")
	assert.NoError(t, err)

	t.Setenv("SQLFLOW_AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;")

	rep, err := Validate(context.Background(), Request{
		Path:   "testdata/issue-120.yml",
		Config: string(src),
	})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)

	var found bool
	for _, d := range rep.Diagnostics {
		if d.Code != "user.config.template_undefined" {
			continue
		}
		found = true
		assert.That(t, contains(d.DidYouMean, "SQLFLOW_AZURE_STORAGE_CONNECTION_STRING"))
		assert.Equal(t, 8, d.Position.Line)
	}
	assert.That(t, found)
}

// Every diagnostic in one pass: a config that is wrong twice reports twice.
func TestValidateReportsEveryDiagnostic_TwoFaultsOneRun(t *testing.T) {
	src := `commands:
  name: not a list
pipeline:
  source:
    type: kafka
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT {{ MISSING_VAR }}
  sink:
    type: console
`
	rep, err := Validate(context.Background(), Request{Config: src})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)

	var template, schema bool
	for _, d := range rep.Diagnostics {
		switch d.Code {
		case "user.config.template_undefined":
			template = true
		case "user.config.invalid", "user.config.parse_failed":
			schema = true
		}
	}
	assert.That(t, template)
	assert.That(t, schema)
}

// A template that will not parse cannot be rendered, so the schema check has
// nothing to inspect. It must report skipped, never pass.
func TestValidateSkipIsExplicit_UnparseableTemplateSkipsSchema(t *testing.T) {
	rep, err := Validate(context.Background(), Request{Config: "x: {{ unclosed\n"})
	assert.NoError(t, err)

	assert.That(t, !rep.OK)
	assert.Equal(t, StatusSkipped, checkStatus(t, rep, "config.schema"))
}

func TestValidateNoSideEffects_CleanConfigIsOK(t *testing.T) {
	src := `pipeline:
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT * FROM batch
  sink:
    type: console
`
	rep, err := Validate(context.Background(), Request{Config: src})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/validate/ -run TestValidate -v`
Expected: FAIL with `undefined: Validate`.

- [ ] **Step 4: Implement Validate**

Create `internal/validate/validate.go`:

```go
package validate

import (
	"context"

	"github.com/turbolytics/sql-flow/internal/config"
)

// Validate checks a config offline and reports everything it finds.
//
// It returns an error only when the request itself is unusable. A config that
// is wrong is a Report with diagnostics, not an error: the caller wants the
// list, and an agent wants all of it in one turn.
func Validate(ctx context.Context, req Request) (Report, error) {
	rep := Report{
		Config:      req.Path,
		Checks:      []Check{},
		Diagnostics: []Diagnostic{},
	}

	provided := config.TemplateVars(req.Vars)

	// Template first: it names variables at their real source positions, and
	// rendering has to succeed before anything else has text to inspect.
	checkTemplate(req.Config, provided, &rep)

	rendered, err := config.RenderTemplateString([]byte(req.Config), req.Vars)
	if err != nil {
		// The template check already reported why. Saying the schema passed
		// here would be a lie: it never ran.
		rep.SetCheck("config.schema", StatusSkipped,
			"the template did not render, so there was no config to check")
		rep.Finish()
		return rep, nil
	}

	checkSchema(rendered, &rep)

	rep.Finish()
	return rep, nil
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/validate/ ./internal/config/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/validate/ internal/config/load.go
git commit -m "validate: one pass, every diagnostic, and a skip that says so"
```

---

### Task 6: The `sqlflow validate` command

**Files:**
- Create: `internal/cli/validate.go`
- Modify: `internal/cli/root.go`
- Test: `internal/cli/validate_test.go`

**Interfaces:**
- Consumes: `validate.Validate`, `validate.Report`, `errs.New`.
- Produces: `func newValidateCommand() *cobra.Command`.

The command returns an `*errs.Error` carrying `CodeConfigInvalid`, which the exit table already maps to `ExitUserError` (10). Wiring `main` to read that table is #161's job, not this plan's: today the binary exits 1 for everything.

- [ ] **Step 1: Write the failing test**

Create `internal/cli/validate_test.go`:

```go
package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/turbolytics/sql-flow/internal/validate"
	"github.com/zeebo/assert"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pipeline.yml")
	assert.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}

const validConfig = `pipeline:
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT * FROM batch
  sink:
    type: console
`

func TestValidateCommand_JSONIsTheContract(t *testing.T) {
	path := writeConfig(t, "topic: {{ MISSING }}\n")

	cmd := newValidateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{path, "--json"})

	err := cmd.Execute()
	assert.Error(t, err)

	var rep validate.Report
	assert.NoError(t, json.Unmarshal(out.Bytes(), &rep))
	assert.That(t, !rep.OK)
	assert.That(t, len(rep.Diagnostics) > 0)
}

func TestValidateCommand_ValidConfigSucceeds(t *testing.T) {
	path := writeConfig(t, validConfig)

	cmd := newValidateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{path})

	assert.NoError(t, cmd.Execute())
	assert.That(t, bytes.Contains(out.Bytes(), []byte("valid")))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/cli/ -run TestValidateCommand -v`
Expected: FAIL with `undefined: newValidateCommand`.

- [ ] **Step 3: Write the command**

Create `internal/cli/validate.go`:

```go
package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/validate"
)

func newValidateCommand() *cobra.Command {
	var asJSON bool

	cmd := &cobra.Command{
		Use:   "validate <config>",
		Short: "Check a pipeline without running it",
		Long: "Check a pipeline offline. validate reaches no broker and no sink, " +
			"executes nothing from the commands block, and reports every fault it " +
			"finds in one pass.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// The config parsed as a command line fine. Usage is noise.
			cmd.SilenceUsage = true

			src, err := os.ReadFile(args[0])
			if err != nil {
				return errs.New(errs.CodeConfigNotFound, "config file not found: %s", args[0])
			}

			rep, err := validate.Validate(cmd.Context(), validate.Request{
				Path:   args[0],
				Config: string(src),
			})
			if err != nil {
				return err
			}

			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				if err := enc.Encode(rep); err != nil {
					return err
				}
			} else {
				writeText(cmd, rep)
			}

			if !rep.OK {
				// Silenced because the report is the message; a second
				// rendering of the same fault helps nobody.
				cmd.SilenceErrors = true
				return errs.New(errs.CodeConfigInvalid, "%s is invalid", args[0])
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit the report as JSON")
	return cmd
}

// writeText renders the report for a human. It says what was skipped as
// plainly as what failed: an unqualified "valid" over checks that never ran
// is the one output this tool must never produce.
func writeText(cmd *cobra.Command, rep validate.Report) {
	out := cmd.OutOrStdout()

	for _, d := range rep.Diagnostics {
		where := ""
		if d.Position != nil {
			where = fmt.Sprintf("%s:%d:%d: ", rep.Config, d.Position.Line, d.Position.Column)
		}
		fmt.Fprintf(out, "%s%s: [%s] %s\n", where, d.Severity, d.Code, d.Message)
		if len(d.DidYouMean) > 0 {
			fmt.Fprintf(out, "    did you mean: %v\n", d.DidYouMean)
		}
		if d.Action != "" {
			fmt.Fprintf(out, "    %s\n", d.Action)
		}
	}

	for _, c := range rep.Checks {
		if c.Status == validate.StatusSkipped {
			fmt.Fprintf(out, "skipped %s: %s\n", c.ID, c.Reason)
		}
	}

	if rep.Vars() != nil && len(rep.Vars().Unused) > 0 {
		fmt.Fprintf(out, "unread variables: %v\n", rep.Vars().Unused)
	}

	if rep.OK {
		fmt.Fprintf(out, "%s: valid\n", rep.Config)
	}
}
```

Add the accessor to `internal/validate/report.go`, since `Variables` is both a field name and a type name:

```go
// Vars returns the variable report, or nil when the template never rendered.
func (r *Report) Vars() *Variables { return r.Variables }
```

- [ ] **Step 4: Register the command**

In `internal/cli/root.go`, beside the existing `AddCommand` calls:

```go
	cmd.AddCommand(newValidateCommand())
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/cli/ -v`
Expected: PASS.

Then run it by hand against the regression fixture:

```bash
go build -o bin/sqlflow ./cmd/sqlflow
./bin/sqlflow validate internal/validate/testdata/issue-120.yml --json
```
Expected: a report naming `SQLFLOW_AZURE_CONNECTION_STRING` as missing, with `SQLFLOW_AZURE_STORAGE_CONNECTION_STRING` in `did_you_mean` when that variable is set in the environment.

- [ ] **Step 6: Commit**

```bash
git add internal/cli/validate.go internal/cli/validate_test.go internal/cli/root.go internal/validate/report.go
git commit -m "cli: sqlflow validate, with JSON as the contract"
```

---

### Task 7: Coverage rows

**Files:**
- Modify: `docs/coverage/features.yml`
- Modify: `docs/coverage/invariants.yml`

**Interfaces:**
- Consumes: the test names from Tasks 3 and 5.
- Produces: nothing in code; the matrix gains four rows.

Test names already carry their attribution: `TestValidateTemplate*` covers `validate.template`, `TestValidateSchema*` covers `validate.schema`, `TestValidateNoSideEffects*` covers `validate.no_side_effects`, `TestValidateReportsEveryDiagnostic*` and `TestValidateSkipIsExplicit*` cover their invariants by prefix.

- [ ] **Step 1: Add the feature rows**

In `docs/coverage/features.yml`, after the `config.validation` entry:

```yaml
  - id: validate.template
    description: Reports referenced, provided, missing, and unused template variables.
    requires: [unit]

  - id: validate.schema
    description: Validates a rendered config against the config JSON Schema, naming the line.
    requires: [unit]
```

- [ ] **Step 2: Add the invariant rows**

In `docs/coverage/invariants.yml`, at the end of the file:

```yaml
  # --- Contract: static validation ------------------------------------------
  - id: validate.no_side_effects
    family: contract
    class: safety
    applies_to: validate_check
    claim: >
      Validation opens no network connection, writes no file, and executes
      nothing from the config's commands block.
    verified_by: named
    requires: []
    enforced: true

  - id: validate.reports_every_diagnostic
    family: contract
    class: liveness
    applies_to: validate_check
    claim: >
      A config with several independent faults reports all of them in one run.
      No check stops at the first failure.
    verified_by: named
    requires: []
    enforced: true

  - id: validate.skip_is_explicit
    family: contract
    class: safety
    applies_to: validate_check
    claim: >
      A check that cannot run reports skipped with a reason. It never reports
      pass, and never reports a failure it cannot substantiate.
    verified_by: named
    requires: []
    enforced: true
    # A model obeys its linter: a spurious error teaches it to edit correct
    # config until the tool goes quiet.
```

- [ ] **Step 3: Regenerate and inspect the matrix**

Run: `make coverage-matrix`
Expected: the four new rows appear, each attributed to the tests above. If the matrix reports a feature with no test, the test name does not match the id's prefix rule — rename the test rather than loosening the registry.

- [ ] **Step 4: Run everything**

Run: `go test ./... -short`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add docs/coverage/features.yml docs/coverage/invariants.yml
git commit -m "coverage: static validation's features and the three it must hold"
```

---

## Notes for the executor

- **`internal/validate` must not import `internal/cli`.** The dependency runs one way, so a control-plane caller can use the package without dragging cobra in.
- **If a check needs a schema it does not have, skip it with a reason.** Never approximate. `sql.bind` in step 2 will be the main user of this rule; the habit starts here.
- **`t.Setenv` is required** in any test touching `SQLFLOW_` variables — `config.TemplateVars` reads the real environment, and a leaked variable makes a neighbouring test fail confusingly.
- **Do not add a dependency.** Everything here is already in `go.mod`.
