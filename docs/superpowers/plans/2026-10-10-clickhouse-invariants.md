# ClickHouse invariants Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every claim on the ClickHouse integration page is a `clickhouse.*` invariant, or a cited existing invariant, measurement or feature, and CI fails when the page and its evidence disagree.

**Architecture:** `applies_to` in `invariants.yml` gains integration ids, so twelve `clickhouse.*` invariants apply to `sink.clickhouse` alone and flow through the existing harness markers, generator and status files. The page moves to `docs/published/clickhouse.mdx` with an anchor comment per claim. A new generator module checks the page against the registries on every run.

**Tech Stack:** Python 3 generator in `scripts/coverage_matrix` (pytest in `tests/tooling`), Go 1.26 tests in `internal/sinks` against testcontainers ClickHouse 26.8.

**Spec:** `docs/superpowers/specs/2026-10-10-clickhouse-invariants-design.md`

## Global Constraints

- Invariant ids start `clickhouse.`; each sets `applies_to: sink.clickhouse`, `family: clickhouse`, `verified_by: harness`.
- Evidence is the existing marker only: `coverage.Invariant(t, "<id>", "sink.clickhouse")`. No attribution by test name.
- Integration tests are named `TestIntegrationSinkClickhouse_*`, skip under `-short`, and use `newLiveClickhouseSink` or `sharedClickhouse`.
- `invariants.yml` stays runtime-only. Tooling claims cite a feature.
- Prose follows the repo's CLAUDE.md: SQLFlow in prose, `sqlflow` for the command.
- Go struct literals put each field on its own line.
- Run go, make, docker and gh outside the sandbox.

## Review Focus

1. An anchor with extra spaces or a trailing space, `{/*  invariant:  id */}`, must still parse. Pinned in Task 2.
2. A `config` block that differs only by its final newline from the published file must pass; any other byte must fail. Pinned in Task 2.
3. `applies_to` naming an integration id that does not exist must be a registry problem, not a silent invariant with no cells. Pinned in Task 1.
4. An exemption in another integration's file against a `clickhouse.*` invariant must be a registry problem. Pinned in Task 1.
5. A published page whose name has no matching invariant namespace, such as a future `motherduck.mdx`, must pass with no "uncited" problems. Pinned in Task 2.

---

### Task 1: `applies_to` accepts an integration id

**Files:**
- Modify: `scripts/coverage_matrix/registries.py` (FAMILIES; `validate_registries`)
- Modify: `scripts/coverage_matrix/invariants.py` (new `targets`; `build_invariants`; `snapshot_invariants`)
- Modify: `scripts/coverage_matrix/page.py` (`page_snapshot`)
- Test: `tests/tooling/test_invariants.py`, `tests/tooling/test_registries.py`

**Interfaces:**
- Produces: `targets(inv: dict, integrations: list[dict]) -> list[dict]` in `coverage_matrix.invariants`: the non-test-only integrations an invariant gets cells for. `applies_to` in `KINDS` selects by kind; otherwise it selects the one integration with that id.
- Produces: `"clickhouse"` in `FAMILIES`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/tooling/test_invariants.py`:

```python
SCOPED = {"id": "clickhouse.enum.null_is_zero", "family": "clickhouse",
          "class": "safety", "applies_to": "sink.clickhouse", "claim": "z",
          "verified_by": "harness", "requires": ["integration"]}


def test_an_invariant_scoped_to_an_integration_gets_one_cell():
    s = inv_snap(invariants=[SCOPED])
    inv = s["invariants"][0]
    assert set(inv["integrations"]) == {"sink.clickhouse"}


def test_a_scoped_marker_from_another_integration_is_unknown():
    s = inv_snap(invariants=[SCOPED], evidence={"integration": {
        "TestX": [("clickhouse.enum.null_is_zero", "sink.console")]}},
        results={"integration": {"TestX": PASS}})
    assert s["unknown_invariant_markers"] == [{
        "test": "TestX", "invariant": "clickhouse.enum.null_is_zero",
        "integration": "sink.console"}]


def test_a_scoped_invariant_with_evidence_is_covered_and_no_gap():
    s = inv_snap(invariants=[SCOPED], evidence={"integration": {
        "TestY": [("clickhouse.enum.null_is_zero", "sink.clickhouse")]}},
        results={"integration": {"TestY": PASS}})
    cell = s["invariants"][0]["integrations"]["sink.clickhouse"]["integration"]
    assert cell["status"] == "covered"
    assert s["invariant_gaps"] == []
```

Append to `tests/tooling/test_registries.py`:

```python
def test_applies_to_may_name_an_integration():
    invs = [dict(INVARIANTS[0], id="clickhouse.x", family="clickhouse",
                 applies_to="sink.clickhouse")]
    assert validate_registries(invs, INTEGRATIONS, FEATURES) == []


def test_applies_to_naming_no_kind_and_no_integration_is_a_problem():
    invs = [dict(INVARIANTS[0], id="clickhouse.x", family="clickhouse",
                 applies_to="sink.clikchouse")]
    problems = validate_registries(invs, INTEGRATIONS, FEATURES)
    assert any("applies_to 'sink.clikchouse'" in p for p in problems)


def test_an_exemption_from_another_integrations_invariant_is_a_problem():
    invs = [dict(INVARIANTS[0], id="clickhouse.x", family="clickhouse",
                 applies_to="sink.clickhouse")]
    integs = [dict(i) for i in INTEGRATIONS]
    console = next(i for i in integs if i["id"] == "sink.console")
    console["exempt"] = [{"invariant": "clickhouse.x", "reason": "r",
                          "proven_by": "TestZ"}]
    problems = validate_registries(invs, integs, FEATURES)
    assert any("sink.console" in p and "clickhouse.x" in p for p in problems)
```

Check the imports at the top of `test_registries.py` include `validate_registries`, `INVARIANTS`, `INTEGRATIONS` and `FEATURES`; add any that are missing (`from samples import *` provides the last three).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --locked pytest tests/tooling/test_invariants.py tests/tooling/test_registries.py -q`
Expected: the six new tests FAIL (no cells for a scoped invariant; `applies_to ... is not one of`; `family 'clickhouse' is not one of`).

- [ ] **Step 3: Implement**

In `registries.py`:

```python
FAMILIES = ("resilience", "checkpoint", "types", "lifecycle", "errors", "cache",
            "clickhouse")
```

In `validate_registries`, build the integration id set before the invariant loop and replace the `applies_to` check:

```python
    integration_ids = {i.get("id") for i in integrations}
    ...
        # applies_to is a kind, which every integration of that kind must
        # prove, or one integration's id, for a claim only it makes.
        if (inv.get("applies_to") not in KINDS
                and inv.get("applies_to") not in integration_ids):
            problems.append(
                f"invariants.yml: {fid} applies_to {inv.get('applies_to')!r} is "
                f"neither one of {KINDS} nor an integration id")
```

Replace the exemption kind check at the end:

```python
            scope = inv.get("applies_to")
            if scope != integ.get("kind") and scope != iid:
                problems.append(
                    f"integrations/{iid}.yml: is exempt from {inv['id']}, which "
                    f"applies_to {scope}")
```

In `invariants.py`, add after `by_kind` and import `KINDS` from `.registries`:

```python
def targets(inv, integrations):
    """The integrations an invariant gets cells for.

    A kind selects every integration of it; an integration id selects that
    one. test_only integrations get none, as in by_kind.
    """
    scope = inv["applies_to"]
    if scope in KINDS:
        return by_kind(integrations).get(scope, [])
    return [i for i in integrations
            if i["id"] == scope and not i.get("test_only")]
```

In `build_invariants`, replace the `allowed` loop body:

```python
    allowed = {}
    for inv in invariants:
        allowed[inv["id"]] = {i["id"] for i in targets(inv, integrations)}
```

and delete the now-unused `kinds` line there. In `snapshot_invariants` replace `for integ in kinds.get(inv["applies_to"], []):` with `for integ in targets(inv, integrations):` and delete its `kinds = by_kind(integrations)` line. In `page.py` `page_snapshot`, do the same replacement, import `targets`, and drop `by_kind` from the import if unused.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --locked pytest tests/tooling -q`
Expected: all pass, including the existing `test_an_invariant_applies_only_to_its_kind`.

- [ ] **Step 5: Commit**

```bash
git add scripts/coverage_matrix tests/tooling
git commit -m "coverage: an invariant may apply to one integration ..."
```

The body names the defect (an integration-only claim had nowhere to live but a parallel registry), the fix, and the evidence (the six tests).

---

### Task 2: The published page check

**Files:**
- Create: `scripts/coverage_matrix/published.py`
- Modify: `scripts/coverage_matrix/registries.py` (paths)
- Modify: `scripts/coverage_matrix/cli.py` (call the check)
- Test: `tests/tooling/test_published.py`

**Interfaces:**
- Consumes: `render_types_page(lattice, integration) -> str` from `coverage_matrix.types`.
- Produces: `validate_published(invariants, features, integrations, lattice, pages_dir=PUBLISHED_DIR, configs_dir=PUBLISHED_CONFIGS, repo=REPO) -> list[str]`.
- Produces: `anchors(text: str) -> list[tuple[str, str]]` returning `(kind, value)` pairs in page order.
- Produces in `registries.py`: `PUBLISHED_DIR = os.path.join(REPO, "docs", "published")`, `PUBLISHED_CONFIGS = os.path.join(REPO, "dev", "config", "examples", "published")`.

Anchor grammar: `{/* <kind>: <value> */}` with `kind` one of `invariant`, `measured`, `config`, `feature`, `generated`. `generated: type-table` precedes the type table; the check compares every `|` row from the anchor to the first blank line against the supported and null rows `render_types_page` emits for `sink.clickhouse`, and the `- ` bullet list after `generated: unsupported-types` against its unsupported list.

- [ ] **Step 1: Write the failing tests**

Create `tests/tooling/test_published.py`:

```python
"""Tests for coverage_matrix.published."""

from coverage_matrix.published import anchors, validate_published
from samples import *  # noqa: F401,F403

INVS = [
    {"id": "clickhouse.a", "applies_to": "sink.clickhouse"},
    {"id": "clickhouse.b", "applies_to": "sink.clickhouse"},
    {"id": "type.null", "applies_to": "sink"},
]
FEATS = [{"id": "cli.validate"}]


def page(tmp_path, name, text, configs=None, files=None):
    pages = tmp_path / "pages"
    pages.mkdir(exist_ok=True)
    (pages / name).write_text(text)
    cfg = tmp_path / "configs"
    cfg.mkdir(exist_ok=True)
    for k, v in (configs or {}).items():
        (cfg / k).write_text(v)
    for k, v in (files or {}).items():
        p = tmp_path / k
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(v)
    return validate_published(INVS, FEATS, [], [], pages_dir=str(pages),
                              configs_dir=str(cfg), repo=str(tmp_path),
                              types_table=False)


def test_anchors_tolerate_spacing():
    assert anchors("x {/*  invariant:  clickhouse.a  */} y") == [
        ("invariant", "clickhouse.a")]


def test_a_page_citing_every_namespaced_invariant_passes(tmp_path):
    text = "{/* invariant: clickhouse.a */}\n{/* invariant: clickhouse.b */}\n"
    assert page(tmp_path, "clickhouse.mdx", text) == []


def test_an_uncited_namespaced_invariant_is_a_problem(tmp_path):
    problems = page(tmp_path, "clickhouse.mdx", "{/* invariant: clickhouse.a */}\n")
    assert any("clickhouse.b" in p and "cited nowhere" in p for p in problems)


def test_an_unknown_invariant_anchor_is_a_problem(tmp_path):
    text = ("{/* invariant: clickhouse.a */}{/* invariant: clickhouse.b */}"
            "{/* invariant: clickhouse.zz */}")
    assert any("clickhouse.zz" in p for p in page(tmp_path, "clickhouse.mdx", text))


def test_a_page_with_no_namespace_needs_no_citations(tmp_path):
    assert page(tmp_path, "motherduck.mdx", "no anchors here\n") == []


def test_a_measured_anchor_must_name_a_file(tmp_path):
    text = ("{/* invariant: clickhouse.a */}{/* invariant: clickhouse.b */}"
            "{/* measured: docs/published/clickhouse/perf.out */}")
    assert any("perf.out" in p for p in page(tmp_path, "clickhouse.mdx", text))
    assert page(tmp_path, "clickhouse.mdx", text,
                files={"docs/published/clickhouse/perf.out": "x"}) == []


def test_a_config_block_must_equal_its_file(tmp_path):
    base = "{/* invariant: clickhouse.a */}{/* invariant: clickhouse.b */}\n"
    text = base + "{/* config: t.yml */}\n```yaml\na: 1\n```\n"
    assert page(tmp_path, "clickhouse.mdx", text, configs={"t.yml": "a: 1\n"}) == []
    assert page(tmp_path, "clickhouse.mdx", text, configs={"t.yml": "a: 1"}) == []
    bad = page(tmp_path, "clickhouse.mdx", text, configs={"t.yml": "a: 2\n"})
    assert any("t.yml" in p and "differs" in p for p in bad)


def test_a_feature_anchor_must_name_a_feature(tmp_path):
    base = "{/* invariant: clickhouse.a */}{/* invariant: clickhouse.b */}"
    assert page(tmp_path, "clickhouse.mdx", base + "{/* feature: cli.validate */}") == []
    assert any("cli.nope" in p for p in
               page(tmp_path, "clickhouse.mdx", base + "{/* feature: cli.nope */}"))
```

Add one test that uses the real registries and the real page, which Task 8 creates, so CI holds the committed page:

```python
def test_the_committed_pages_pass():
    from coverage_matrix.registries import (load_features, load_integrations,
                                            load_invariants, load_lattice)
    assert validate_published(load_invariants(), load_features(),
                              load_integrations(), load_lattice()) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --locked pytest tests/tooling/test_published.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'coverage_matrix.published'`.

- [ ] **Step 3: Implement `published.py`**

```python
"""Published pages and the claims they make.

A page under docs/published carries an anchor comment beside each claim. The
check holds the page to the registries: every anchor resolves, every invariant
in the page's namespace is cited, and every embedded config and generated
table matches its source. A page that drifts from its evidence fails here
rather than in a reader's pipeline.
"""

import os
import re

from .registries import PUBLISHED_CONFIGS, PUBLISHED_DIR, REPO
from .types import render_types_page

ANCHOR = re.compile(
    r"\{/\*\s*(invariant|measured|config|feature|generated)\s*:\s*(.+?)\s*\*/\}")
FENCE = re.compile(r"```[a-z]*\n(.*?)```", re.S)


def anchors(text):
    return [(m.group(1), m.group(2)) for m in ANCHOR.finditer(text)]


def _block_after(text, end):
    """The first fenced block after offset end, without its fences."""
    m = FENCE.search(text, end)
    return m.group(1) if m else None


def _rows_after(text, end, prefix):
    """Consecutive lines starting with prefix after offset end."""
    rows, started = [], False
    for line in text[end:].splitlines():
        if line.startswith(prefix):
            rows.append(line)
            started = True
        elif started:
            break
    return rows


def _generated(lattice, integrations):
    integ = next((i for i in integrations if i["id"] == "sink.clickhouse"), None)
    if integ is None:
        return [], []
    lines = render_types_page(lattice, integ).splitlines()
    table = [l for l in lines if l.startswith("| `")]
    unsupported = [l for l in lines if l.startswith("- ")]
    return table, unsupported


def validate_published(invariants, features, integrations, lattice,
                       pages_dir=PUBLISHED_DIR, configs_dir=PUBLISHED_CONFIGS,
                       repo=REPO, types_table=True):
    problems = []
    if not os.path.isdir(pages_dir):
        return problems
    inv_ids = {i["id"] for i in invariants}
    feat_ids = {f["id"] for f in features}
    table, unsupported = (_generated(lattice, integrations)
                          if types_table else ([], []))

    for name in sorted(os.listdir(pages_dir)):
        if not name.endswith(".mdx"):
            continue
        rel = os.path.relpath(os.path.join(pages_dir, name), repo)
        with open(os.path.join(pages_dir, name)) as fh:
            text = fh.read()
        cited = set()
        for m in ANCHOR.finditer(text):
            kind, value = m.group(1), m.group(2)
            if kind == "invariant":
                cited.add(value)
                if value not in inv_ids:
                    problems.append(f"{rel}: cites invariant {value}, which "
                                    "invariants.yml does not declare")
            elif kind == "feature" and value not in feat_ids:
                problems.append(f"{rel}: cites feature {value}, which "
                                "features.yml does not declare")
            elif kind == "measured" and not os.path.isfile(os.path.join(repo, value)):
                problems.append(f"{rel}: cites measurement {value}, which does not exist")
            elif kind == "config":
                path = os.path.join(configs_dir, value)
                block = _block_after(text, m.end())
                if not os.path.isfile(path):
                    problems.append(f"{rel}: embeds config {value}, which does not exist")
                elif block is None or block.rstrip("\n") != open(path).read().rstrip("\n"):
                    problems.append(f"{rel}: the block after config {value} differs "
                                    "from the published file")
            elif kind == "generated" and types_table:
                want = table if value == "type-table" else unsupported
                prefix = "| `" if value == "type-table" else "- "
                if _rows_after(text, m.end(), prefix) != want:
                    problems.append(f"{rel}: the {value} differs from the one "
                                    "the type declaration generates")
        namespace = name[:-len(".mdx")] + "."
        for iid in sorted(inv_ids):
            if iid.startswith(namespace) and iid not in cited:
                problems.append(f"{rel}: invariant {iid} is cited nowhere on the page")
    return problems
```

Add the two paths to `registries.py` beside `TYPES_PAGE`. In `cli.py`, import `validate_published` and add after `validate_types`:

```python
    problems += validate_published(invariants, features, integrations, lattice)
```

- [ ] **Step 4: Run the tests**

Run: `uv run --locked pytest tests/tooling/test_published.py -q -k "not committed"`
Expected: PASS. `test_the_committed_pages_pass` passes vacuously until Task 8 adds the page, because the directory is absent.

- [ ] **Step 5: Commit**

`coverage: a published page is held to the claims it cites`, with the defect, fix and evidence in the body.

---

### Task 3: The generated type table matches what the reviewer accepted

**Files:**
- Modify: `scripts/coverage_matrix/types.py` (`render_types_page`)
- Modify: `docs/coverage/integrations/sink.clickhouse.yml` (`utf8` gains `rule`; `nulls.default.rule`)
- Regenerate: `docs/coverage/clickhouse-types.mdx` with `make coverage-page`
- Test: `tests/tooling/test_types.py`

**Interfaces:**
- Produces: a supported row whose declaration has any column carrying `value` renders the lattice entry's first DuckDB cast alone, and its notes read `<rule> <the other casts> reach the sink as <first cast>.`

- [ ] **Step 1: Write the failing test**

```python
def test_a_text_carrier_row_names_its_cast_alone_and_says_the_rest_reach_it():
    lattice = [{"key": "utf8", "duckdb": ["VARCHAR", "UUID", "JSON"]}]
    integ = {"types": {"utf8": {"outcome": "exact",
                                "rule": "the text must already be the column's text form.",
                                "columns": [{"type": "String", "expect": "x"},
                                            {"type": "UUID", "value": "u", "expect": "u"}]}}}
    page = render_types_page(lattice, integ)
    assert ("| `VARCHAR` | `String`, `UUID` | the text must already be the "
            "column's text form. `UUID` and `JSON` reach the sink as `VARCHAR`. |") in page
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --locked pytest tests/tooling/test_types.py -q`
Expected: FAIL, the row still lists `VARCHAR`, `UUID`, `JSON`.

- [ ] **Step 3: Implement**

In `render_types_page`, replace the supported-row lines:

```python
        cols = ", ".join("`%s`" % c["type"] for c in decl.get("columns", []))
        rule = decl.get("rule", "")
        # A carrier whose columns each take their own value proves the pair,
        # not the cross-product: a UUID cast reaches a Decimal column only as
        # text the Decimal parses. Name the cast the matrix wrote, and say the
        # others arrive as it.
        if any(c.get("value") for c in decl.get("columns", [])) and len(entry["duckdb"]) > 1:
            first, rest = entry["duckdb"][0], entry["duckdb"][1:]
            casts = "`%s`" % first
            rule = ("%s %s reach the sink as `%s`." % (
                rule, " and ".join("`%s`" % r for r in rest), first)).strip()
        supported.append("| %s | %s | %s |" % (casts, cols, rule))
```

In `sink.clickhouse.yml`, under `utf8:` add `rule: the text must already be the column's text form; see the note below.` and set `nulls.default.rule` to:

```yaml
    rule: the column type's zero value; the DEFAULT expression is not evaluated because every produced column is named in the INSERT. For an Enum with no 0 member that value is not a member, and the row cannot be read
```

- [ ] **Step 4: Run the tests and regenerate**

Run: `uv run --locked pytest tests/tooling -q && make coverage-page`
Expected: PASS; `docs/coverage/clickhouse-types.mdx` shows the new `VARCHAR` row and NULL row.

- [ ] **Step 5: Commit**

`coverage: the type table names VARCHAR alone, and NULL into an Enum`.

---

### Task 4: The unit invariants, DSN scheme and TLS

**Files:**
- Modify: `docs/coverage/invariants.yml` (new `# --- ClickHouse` section with all twelve entries)
- Create: `internal/sinks/clickhouse_invariants_test.go`

**Interfaces:**
- Consumes: `clickhouseOptions(dsn string) (*clickhouse.Options, error)` and `config.ClickhouseSink` in package `sinks`.
- Produces: the twelve invariant ids below, used by every later task and by the page.

- [ ] **Step 1: Declare all twelve invariants**

Append to `invariants.yml`, before `# --- Lifecycle`, one entry per row of the spec's table, in this shape:

```yaml
  # --- ClickHouse ----------------------------------------------------------
  # Claims only sink.clickhouse makes. Each one backs a sentence on
  # docs/published/clickhouse.mdx, which cites it by id, and the page check
  # fails when the two disagree.
  - id: clickhouse.dsn.scheme
    family: clickhouse
    class: safety
    applies_to: sink.clickhouse
    claim: >
      The DSN scheme decides the protocol, TLS and the default port. `@` and
      `:` in a password parse raw; `/`, `#` and `?` must be percent-encoded.
    verified_by: harness
    requires: [unit]
```

`clickhouse.dsn.scheme` and `clickhouse.tls.verifies` require `[unit]`; the other ten require `[integration]`. Claims are the spec's text verbatim.

- [ ] **Step 2: Write the failing unit tests**

Create `internal/sinks/clickhouse_invariants_test.go`:

```go
package sinks

// The clickhouse.* invariants: claims only the ClickHouse sink makes, each
// one a sentence on docs/published/clickhouse.mdx. A test here that fails is
// a page that is now wrong.

import (
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestSinkClickhouse_DSNSchemeDecidesProtocolTLSAndPort(t *testing.T) {
	coverage.Invariant(t, "clickhouse.dsn.scheme", "sink.clickhouse")

	cases := []struct {
		dsn      string
		protocol clickhouse.Protocol
		tls      bool
		addr     string
	}{
		{dsn: "clickhouses://h/db", protocol: clickhouse.HTTP, tls: true, addr: "h:8443"},
		{dsn: "https://h/db", protocol: clickhouse.HTTP, tls: true, addr: "h:8443"},
		{dsn: "clickhouse://h/db", protocol: clickhouse.HTTP, tls: false, addr: "h:8123"},
		{dsn: "http://h/db", protocol: clickhouse.HTTP, tls: false, addr: "h:8123"},
		{dsn: "natives://h/db", protocol: clickhouse.Native, tls: true, addr: "h:9440"},
		{dsn: "tcp://h/db", protocol: clickhouse.Native, tls: false, addr: "h:9000"},
		{dsn: "native://h/db", protocol: clickhouse.Native, tls: false, addr: "h:9000"},
	}
	for _, c := range cases {
		opts, err := clickhouseOptions(c.dsn)
		assert.NoError(t, err)
		assert.Equal(t, c.protocol, opts.Protocol)
		assert.Equal(t, c.tls, opts.TLS != nil)
		assert.Equal(t, []string{c.addr}, opts.Addr)
	}

	// @ and : in a password parse raw.
	opts, err := clickhouseOptions("clickhouses://u:p@ss:w0rd@h/db")
	assert.NoError(t, err)
	assert.Equal(t, "p@ss:w0rd", opts.Auth.Password)

	// # left raw ends the URL early and reads as a port.
	_, err = clickhouseOptions("clickhouses://u:pa#ss@h/db")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid port")

	// Percent-encoded, the same password parses.
	opts, err = clickhouseOptions("clickhouses://u:pa%23ss@h/db")
	assert.NoError(t, err)
	assert.Equal(t, "pa#ss", opts.Auth.Password)
}

func TestSinkClickhouse_TLSVerifiesAgainstTheSystemRoots(t *testing.T) {
	coverage.Invariant(t, "clickhouse.tls.verifies", "sink.clickhouse")

	opts, err := clickhouseOptions("clickhouses://h/db")
	assert.NoError(t, err)
	assert.False(t, opts.TLS.InsecureSkipVerify)
	assert.Nil(t, opts.TLS.RootCAs)

	// No option skips verification: the sink's config is a DSN and a table.
	// A field added here is a page claim to revisit.
	var names []string
	typ := reflect.TypeOf(config.ClickhouseSink{})
	for i := 0; i < typ.NumField(); i++ {
		names = append(names, typ.Field(i).Name)
	}
	assert.DeepEqual(t, []string{"DSN", "Table"}, names)
}
```

- [ ] **Step 3: Run them**

Run: `go test -short -run 'TestSinkClickhouse_(DSNScheme|TLSVerifies)' ./internal/sinks -v`
Expected: PASS, since the behavior exists. If the `#` case reports a different message, set the assertion to the observed text and say so in the commit body: the page quotes it.

- [ ] **Step 4: Run the short suite**

Run: `go test -short -race ./... 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Commit**

`invariants: twelve clickhouse.* claims, and the DSN and TLS proofs`.

---

### Task 5: Columns, tables, Enum NULLs, per-destination text

**Files:**
- Modify: `internal/sinks/clickhouse_invariants_test.go`
- Modify: `internal/sinks/conformance_types_test.go` (one marker line)

**Interfaces:**
- Consumes: `newLiveClickhouseSink(t *testing.T, ddl string) *ClickhouseSink`, where `ddl` carries one `%s` for the table name, and `s.conn`, `s.table`.
- Produces: `textColumns(t *testing.T, cols ...textCol) arrow.Table` and `type textCol struct { name string; values []*string }`, used by Tasks 6 and 7.

- [ ] **Step 1: Write the tests**

Add the helper and three tests:

```go
type textCol struct {
	name   string
	values []*string
}

func str(s string) *string { return &s }

// textColumns builds one batch of nullable utf8 columns, in the order given.
func textColumns(t *testing.T, cols ...textCol) arrow.Table {
	t.Helper()
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i] = arrow.Field{
			Name:     c.name,
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		}
	}
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for i, c := range cols {
		sb := b.Field(i).(*array.StringBuilder)
		for _, v := range c.values {
			if v == nil {
				sb.AppendNull()
			} else {
				sb.Append(*v)
			}
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	t.Cleanup(tbl.Release)
	return tbl
}

func flushOnce(t *testing.T, s core.Sink, tbl arrow.Table) error {
	t.Helper()
	assert.NoError(t, s.WriteTable(context.Background(), tbl))
	return s.Flush(context.Background())
}

func TestIntegrationSinkClickhouse_InsertsByName(t *testing.T) {
	coverage.Invariant(t, "clickhouse.insert.by_name", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (
		b String, a String, d String DEFAULT 'omitted-default'
	) ENGINE = MergeTree() ORDER BY tuple()`)

	// Produced in the opposite order to the table, and without d.
	assert.NoError(t, flushOnce(t, s, textColumns(t,
		textCol{name: "a", values: []*string{str("A")}},
		textCol{name: "b", values: []*string{str("B")}})))
	var a, b, d string
	assert.NoError(t, s.conn.QueryRow(context.Background(),
		"SELECT a, b, d FROM "+s.table).Scan(&a, &b, &d))
	assert.Equal(t, "A", a)
	assert.Equal(t, "B", b)
	assert.Equal(t, "omitted-default", d)

	// A column the table lacks fails the batch, naming it.
	err := flushOnce(t, s, textColumns(t,
		textCol{name: "a", values: []*string{str("A")}},
		textCol{name: "extra", values: []*string{str("X")}}))
	assert.Error(t, err)
	assert.True(t, errs.HasCode(err, errs.CodeSinkWriteFailed))
	assert.Contains(t, err.Error(), "column extra is not present in the table")
}

func TestIntegrationSinkClickhouse_NeverCreatesATable(t *testing.T) {
	coverage.Invariant(t, "clickhouse.table.not_created", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ch := sharedClickhouse(t)
	table := fmt.Sprintf("absent_%d", time.Now().UnixNano())
	s := mustDirectSink(t, ch, table)

	err := flushOnce(t, s, textColumns(t, textCol{name: "a", values: []*string{str("A")}}))
	assert.Error(t, err)
	assert.True(t, errs.HasCode(err, errs.CodeSinkWriteFailed))

	var exists uint8
	assert.NoError(t, s.conn.QueryRow(context.Background(),
		"EXISTS TABLE "+table).Scan(&exists))
	assert.Equal(t, uint8(0), exists)
}

func TestIntegrationSinkClickhouse_EnumNullIsZero(t *testing.T) {
	coverage.Invariant(t, "clickhouse.enum.null_is_zero", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	null := textColumns(t, textCol{name: "e", values: []*string{nil}})
	ctx := context.Background()

	// No 0 member: the INSERT succeeds and the row cannot be read.
	noZero := newLiveClickhouseSink(t,
		`CREATE TABLE %s (e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree() ORDER BY tuple()`)
	assert.NoError(t, flushOnce(t, noZero, null))
	var n uint64
	assert.NoError(t, noZero.conn.QueryRow(ctx, "SELECT count() FROM "+noZero.table).Scan(&n))
	assert.Equal(t, uint64(1), n)
	var e string
	err := noZero.conn.QueryRow(ctx, "SELECT toString(e) FROM "+noZero.table).Scan(&e)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "UNKNOWN_ELEMENT_OF_ENUM")
	var raw int8
	assert.NoError(t, noZero.conn.QueryRow(ctx, "SELECT CAST(e AS Int8) FROM "+noZero.table).Scan(&raw))
	assert.Equal(t, int8(0), raw)

	// A 0 member: the NULL reads as that member.
	zero := newLiveClickhouseSink(t,
		`CREATE TABLE %s (e Enum8('' = 0, 'a' = 1, 'b' = 2)) ENGINE = MergeTree() ORDER BY tuple()`)
	assert.NoError(t, flushOnce(t, zero, null))
	assert.NoError(t, zero.conn.QueryRow(ctx, "SELECT toString(e) FROM "+zero.table).Scan(&e))
	assert.Equal(t, "", e)

	// Nullable: the NULL reads as NULL.
	nullable := newLiveClickhouseSink(t,
		`CREATE TABLE %s (e Nullable(Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree() ORDER BY tuple()`)
	assert.NoError(t, flushOnce(t, nullable, null))
	var isNull uint8
	assert.NoError(t, nullable.conn.QueryRow(ctx, "SELECT e IS NULL FROM "+nullable.table).Scan(&isNull))
	assert.Equal(t, uint8(1), isNull)
}
```

In `conformance_types_test.go`, after `coverage.Covers(t, "sink.clickhouse")`, add:

```go
	// The type table proves each text destination with that destination's own
	// value, which is the claim the page makes about VARCHAR.
	coverage.Invariant(t, "clickhouse.type_text.per_destination", "sink.clickhouse")
```

Add imports as needed: `context`, `fmt`, `time`, `arrow`, `array`, `memory`, `core`, `errs`.

- [ ] **Step 2: Run them**

Run: `go test -run 'TestIntegrationSinkClickhouse_(InsertsByName|NeverCreatesATable|EnumNullIsZero|Types)$' ./internal/sinks -v -timeout 10m`
Expected: PASS. A failure is a finding about the page claim, not a test to relax: report it.

- [ ] **Step 3: Run the short suite, then commit**

Run: `go test -short -race ./... 2>&1 | tail -5`, then commit `invariants: ClickHouse matches columns by name, creates no table, and stores a NULL Enum as 0`.

---

### Task 6: Encode failures, text timestamps, Decimal text

**Files:**
- Modify: `internal/sinks/clickhouse_invariants_test.go`

**Interfaces:**
- Consumes: `textColumns`, `textCol`, `str`, `flushOnce` from Task 5; `newRetrying(inner core.Sink, policy RetryPolicy) *retrying` with its `onRetry` field; `errs.ExitCode(error) int`, `errs.ExitUserError`.

- [ ] **Step 1: Write the tests**

```go
func TestIntegrationSinkClickhouse_EncodeFailureIsNotRetried(t *testing.T) {
	coverage.Invariant(t, "clickhouse.encode.fails_once", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (ts DateTime) ENGINE = MergeTree() ORDER BY tuple()`)
	r := newRetrying(s, RetryPolicy{
		MaxAttempts:    4,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		Deadline:       time.Minute,
	})
	retries := 0
	r.onRetry = func(int, error) { retries++ }

	err := flushOnce(t, r, textColumns(t,
		textCol{name: "ts", values: []*string{str("2026-09-01T12:00:00Z")}}))
	assert.Error(t, err)
	assert.True(t, errs.HasCode(err, errs.CodeSinkEncodeFailed))
	assert.Equal(t, 0, retries)
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
}

func TestIntegrationSinkClickhouse_TemporalTextTakesTwoShapes(t *testing.T) {
	coverage.Invariant(t, "clickhouse.temporal_text.two_shapes", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (k String, ts DateTime('UTC')) ENGINE = MergeTree() ORDER BY k`)
	assert.NoError(t, flushOnce(t, s, textColumns(t,
		textCol{name: "k", values: []*string{str("plain"), str("offset")}},
		textCol{name: "ts", values: []*string{str("2026-09-01 12:00:00"), str("2026-09-01 12:00:00 +09:00")}})))

	got := map[string]string{}
	rows, err := s.conn.Query(ctx, "SELECT k, toString(ts) FROM "+s.table)
	assert.NoError(t, err)
	for rows.Next() {
		var k, ts string
		assert.NoError(t, rows.Scan(&k, &ts))
		got[k] = ts
	}
	assert.NoError(t, rows.Err())
	assert.Equal(t, "2026-09-01 12:00:00", got["plain"])
	assert.Equal(t, "2026-09-01 03:00:00", got["offset"])

	err = flushOnce(t, s, textColumns(t,
		textCol{name: "k", values: []*string{str("iso")}},
		textCol{name: "ts", values: []*string{str("2026-09-01T12:00:00Z")}}))
	assert.Error(t, err)
	assert.True(t, errs.HasCode(err, errs.CodeSinkEncodeFailed))
}

func TestIntegrationSinkClickhouse_DecimalTakesText(t *testing.T) {
	coverage.Invariant(t, "clickhouse.decimal.text_only", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (amount Decimal(10, 2)) ENGINE = MergeTree() ORDER BY tuple()`)
	assert.NoError(t, flushOnce(t, s, textColumns(t,
		textCol{name: "amount", values: []*string{str("12.34")}})))
	var got string
	assert.NoError(t, s.conn.QueryRow(ctx, "SELECT toString(amount) FROM "+s.table).Scan(&got))
	assert.Equal(t, "12.34", got)

	schema := arrow.NewSchema([]arrow.Field{{
		Name: "amount",
		Type: arrow.PrimitiveTypes.Float64,
	}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Float64Builder).Append(12.34)
	rec := b.NewRecord()
	defer rec.Release()
	floats := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer floats.Release()
	err := flushOnce(t, s, floats)
	assert.Error(t, err)
	assert.True(t, errs.HasCode(err, errs.CodeSinkEncodeFailed))
}
```

- [ ] **Step 2: Run them**

Run: `go test -run 'TestIntegrationSinkClickhouse_(EncodeFailureIsNotRetried|TemporalTextTakesTwoShapes|DecimalTakesText)$' ./internal/sinks -v -timeout 10m`
Expected: PASS.

- [ ] **Step 3: Run the short suite, then commit**

`invariants: an encode failure fails once, and text reaches DateTime and Decimal only in their forms`.

---

### Task 7: Replays, one INSERT per flush, wrong credentials

**Files:**
- Modify: `internal/sinks/clickhouse_invariants_test.go`

**Interfaces:**
- Consumes: Task 5 helpers; `sharedClickhouse`, `clickhouseDSN`, `NewClickhouseSink`, `(*ClickhouseSink).Probe`.

- [ ] **Step 1: Write the tests**

```go
func TestIntegrationSinkClickhouse_ReplayDuplicatesUntilMerge(t *testing.T) {
	coverage.Invariant(t, "clickhouse.replay.duplicates_until_merge", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	batch := func() arrow.Table {
		return textColumns(t,
			textCol{name: "id", values: []*string{str("1"), str("2")}},
			textCol{name: "v", values: []*string{str("first"), str("first")}})
	}
	count := func(s *ClickhouseSink, suffix string) uint64 {
		var n uint64
		assert.NoError(t, s.conn.QueryRow(ctx, "SELECT count() FROM "+s.table+suffix).Scan(&n))
		return n
	}

	// A replay into ReplacingMergeTree: both copies until a merge.
	rmt := newLiveClickhouseSink(t, `CREATE TABLE %s (id String, v String) ENGINE = ReplacingMergeTree ORDER BY id`)
	assert.NoError(t, flushOnce(t, rmt, batch()))
	assert.NoError(t, flushOnce(t, rmt, batch()))
	assert.Equal(t, uint64(4), count(rmt, ""))
	assert.Equal(t, uint64(2), count(rmt, " FINAL"))
	assert.NoError(t, rmt.conn.Exec(ctx, "OPTIMIZE TABLE "+rmt.table+" FINAL"))
	assert.Equal(t, uint64(2), count(rmt, ""))

	// An identical block inside the deduplication window is dropped at insert.
	dedup := newLiveClickhouseSink(t, `CREATE TABLE %s (id String, v String) ENGINE = MergeTree ORDER BY id
		SETTINGS non_replicated_deduplication_window = 100`)
	assert.NoError(t, flushOnce(t, dedup, batch()))
	assert.NoError(t, flushOnce(t, dedup, batch()))
	assert.Equal(t, uint64(2), count(dedup, ""))
}

func TestIntegrationSinkClickhouse_OneFlushIsOneInsert(t *testing.T) {
	coverage.Invariant(t, "clickhouse.flush.one_insert", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (a String) ENGINE = MergeTree ORDER BY tuple()`)
	for _, v := range []string{"x", "y", "z"} {
		assert.NoError(t, s.WriteTable(ctx, textColumns(t, textCol{name: "a", values: []*string{str(v)}})))
	}
	assert.NoError(t, s.Flush(ctx))

	assert.NoError(t, s.conn.Exec(ctx, "SYSTEM FLUSH LOGS"))
	var inserts, rows uint64
	assert.NoError(t, s.conn.QueryRow(ctx, `
		SELECT count(), sum(written_rows) FROM system.query_log
		WHERE type = 'QueryFinish' AND query_kind = 'Insert'
		  AND has(tables, currentDatabase() || '.' || ?)`, s.table).Scan(&inserts, &rows))
	assert.Equal(t, uint64(1), inserts)
	assert.Equal(t, uint64(3), rows)
}

func TestIntegrationSinkClickhouse_WrongPasswordFailsTheProbe(t *testing.T) {
	coverage.Invariant(t, "clickhouse.auth.fails_start", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	ch := sharedClickhouse(t)
	host, err := ch.Host(ctx)
	assert.NoError(t, err)
	port, err := ch.MappedPort(ctx, "8123/tcp")
	assert.NoError(t, err)

	dsn := fmt.Sprintf("clickhouse://%s:wrong@%s/%s",
		clickhouseUser, net.JoinHostPort(host, port.Port()), clickhouseDatabase)
	s, err := NewClickhouseSink(config.ClickhouseSink{
		DSN:   dsn,
		Table: "never_written",
	})
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	err = s.Probe(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "401")
	assert.Contains(t, err.Error(), "Authentication failed")
}
```

If `SYSTEM FLUSH LOGS` or `system.query_log` is denied to the test user, grant the shared container's user access in `sharedClickhouse` with an `--env CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` option or a `GRANT` statement run as default, and say which in the commit body. Record the code number the local server returns for the wrong password; the claim and the page name 401 and the code the test observes.

- [ ] **Step 2: Run them**

Run: `go test -run 'TestIntegrationSinkClickhouse_(ReplayDuplicatesUntilMerge|OneFlushIsOneInsert|WrongPasswordFailsTheProbe)$' ./internal/sinks -v -timeout 10m`
Expected: PASS.

- [ ] **Step 3: Run the short suite, then commit**

`invariants: replays read twice until a merge, a flush is one INSERT, and wrong credentials fail the probe`.

---

### Task 8: The page moves in, with an anchor per claim

**Files:**
- Create: `docs/published/clickhouse.mdx` (from `sqlflow.mdx` on branch `docs-status/clickhouse-117740`, path `docs/superpowers/status/clickhouse-117740/sqlflow.mdx`)
- Create: `docs/published/clickhouse/` holding `perf.sh`, `perfcloud.sh`, `perf.out`, `perfcloud.out`, `cloudverify.out`, `throughput-bisect.out` from the same branch, and `README.md`
- Create: `docs/published/README.md`
- Modify: `dev/config/examples/published/README.md` (the page now lives in `docs/published`)
- Modify: `.github/workflows/ci.yml` (re-include `docs/published/**`)

- [ ] **Step 1: Copy the page and its evidence**

```bash
git show origin/docs-status/clickhouse-117740:docs/superpowers/status/clickhouse-117740/sqlflow.mdx > docs/published/clickhouse.mdx
for f in perf.sh perfcloud.sh perf.out perfcloud.out cloudverify.out throughput-bisect.out; do
  git show origin/docs-status/clickhouse-117740:docs/superpowers/status/clickhouse-117740/$f > docs/published/clickhouse/$f
done
chmod +x docs/published/clickhouse/*.sh
```

Fix the scripts' `W=` paths to read `taxi.yml` from `dev/config/examples/published/clickhouse.taxi.yml`.

- [ ] **Step 2: Add the anchors**

One anchor per claim, placed after the sentence it backs. The required set:

| Section | Anchors |
| --- | --- |
| Setup scheme table | `invariant: clickhouse.dsn.scheme` |
| "The target table must already exist ... match the table's column names" | `invariant: clickhouse.table.not_created`, `invariant: clickhouse.insert.by_name` |
| Authentication: special characters | `invariant: clickhouse.dsn.scheme` |
| Authentication: certificate verified, no skip | `invariant: clickhouse.tls.verifies` |
| Step 3 YAML | `config: clickhouse.taxi.yml` before the block |
| Bluesky YAML | `config: clickhouse.bluesky.yml` before the block |
| "Kafka offsets are committed only after each INSERT" | `invariant: pipeline.commit.after_flush` |
| Verify output, Cloud and self-hosted counts | `measured: docs/published/clickhouse/cloudverify.out` |
| Type table | `generated: type-table` before the table; `invariant: type.roundtrip`, `invariant: type.null`, `invariant: clickhouse.type_text.per_destination` after it |
| Text-form note | `invariant: clickhouse.type_text.per_destination` |
| Enum warning | `invariant: clickhouse.enum.null_is_zero` |
| Timestamp note | `invariant: clickhouse.temporal_text.two_shapes`, `invariant: type.timestamp.instant` |
| Decimal note | `invariant: clickhouse.decimal.text_only` |
| Unsupported list | `generated: unsupported-types` before the list; `invariant: type.undeclared.fails_loud` |
| Limits: no table creation | `invariant: clickhouse.table.not_created`, `invariant: clickhouse.insert.by_name` |
| Limits: NULL | `invariant: type.null`, `invariant: clickhouse.enum.null_is_zero` |
| Limits: at-least-once | `invariant: pipeline.commit.after_flush`, `invariant: clickhouse.replay.duplicates_until_merge` |
| Limits: retries | `invariant: clickhouse.encode.fails_once`, `invariant: sink.probe.fails_start` |
| Limits: one connection | `invariant: clickhouse.flush.one_insert` |
| Limits: windows | `invariant: pipeline.window.counts_every_row`, `feature: config.validation` |
| Performance tables | `measured: docs/published/clickhouse/perf.out`, `measured: docs/published/clickhouse/perfcloud.out` |
| Troubleshooting: 401 | `invariant: clickhouse.auth.fails_start` |
| Troubleshooting: missing column | `invariant: clickhouse.insert.by_name` |
| Troubleshooting: Decimal | `invariant: clickhouse.decimal.text_only` |
| Troubleshooting: timestamp | `invariant: clickhouse.temporal_text.two_shapes`, `invariant: clickhouse.encode.fails_once` |
| Troubleshooting: no rows | `invariant: pipeline.flush.eventually`, `invariant: lifecycle.drain.on_cancel` |

Set the page's type-table rows and unsupported list to exactly what `docs/coverage/clickhouse-types.mdx` renders after Task 3. The lateness warning is `sqlflow validate` behavior, so it cites `config.validation`, the feature that covers semantic validation.

- [ ] **Step 3: Re-include the page in CI**

In `.github/workflows/ci.yml`, under `pull_request.paths`, after `- "docs/coverage/**"`, add `- "docs/published/**"`, and extend the comment above it: docs/published holds pages the page check guards.

- [ ] **Step 4: Run the page check**

Run: `uv run --locked pytest tests/tooling -q`
Expected: PASS, including `test_the_committed_pages_pass`. Each failure it prints names the anchor or claim to fix.

- [ ] **Step 5: Commit**

`docs: the ClickHouse page lives here, with an anchor per claim`.

---

### Task 9: Regenerate the coverage status and page

**Files:**
- Regenerate: `docs/coverage/status/*.yml`, `docs/coverage/matrix.md`, `docs/coverage/clickhouse-types.mdx`

- [ ] **Step 1: Produce the unit and integration reports**

```bash
mkdir -p .coverage
CGO_ENABLED=1 go test -short -race -json ./... > .coverage/go.json 2>&1
go test -json -timeout 30m -run '^TestIntegration' ./internal/sinks/... > .coverage/go-integration.json 2>&1
```

The integration pass runs every package, as CI's does, so the status files
match what CI will report. Run it outside the sandbox; it takes up to 30
minutes. Replace `./internal/sinks/...` above with `./...`.

- [ ] **Step 2: Write and check**

Run: `make coverage-write COVERAGE_NOT_RUN="--not-run growth --not-run release" && make coverage-check COVERAGE_NOT_RUN="--not-run growth --not-run release"`
Expected: PASS, with every `clickhouse.*` cell covered at its required level.

- [ ] **Step 3: Commit**

`coverage: twelve clickhouse.* invariants, each covered`.

---

### Task 10: The pull request

- [ ] **Step 1:** Push `feat/clickhouse-invariants` and open one PR against main. The body states the defect (page claims had no CI evidence; five went stale unnoticed), the fix, the evidence (the tests and the page check), and what breaks if wrong. It notes that PR #329's hand-run scripts are superseded and #329 can close once this merges.
- [ ] **Step 2:** Watch CI to green. Do not merge.
