#!/usr/bin/env python3
"""Build the feature coverage matrix from test suite output.

Two axes, deliberately independent:

  Feature  what sqlflow does, e.g. sink.iceberg. Declared in
           docs/coverage/features.yml. Never encodes a test level.
  Level    where the test ran: `unit` for go test -short, `integration` for
           the Go tests that need a real service, `release` for the image
           suite. Derived from which file the result came out of, never
           declared, so it cannot drift from reality.

Keeping them separate is what lets the matrix answer the question that went
unanswered for months: which features are covered by a unit test but never
proven against a real broker or the shipped image.

A skipped test does not cover its feature. It reports as SKIP, because a skip
that reads as coverage is exactly how sink.iceberg shipped untested.

Usage:
    coverage_matrix.py --go go.json --go-integration it.json \
        --pytest pytest.json --write
    coverage_matrix.py --go go.json --go-integration it.json \
        --pytest pytest.json --check

`--check` exits non-zero when a feature is missing a level it requires.
"""

import argparse
import json
import os
import re
import sys

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REGISTRY = os.path.join(REPO, "docs", "coverage", "features.yml")
INVARIANTS = os.path.join(REPO, "docs", "coverage", "invariants.yml")
INTEGRATIONS = os.path.join(REPO, "docs", "coverage", "integrations.yml")
LATTICE = os.path.join(REPO, "docs", "coverage", "lattice.yml")
# The JSON is the artifact CI diffs; the markdown is a view rendered from it.
MATRIX_JSON = os.path.join(REPO, "docs", "coverage", "matrix.json")
MATRIX_MD = os.path.join(REPO, "docs", "coverage", "matrix.md")
# The fragment the ClickHouse integration page publishes. Generated here so it
# cannot drift from the declaration the type runner judges.
TYPES_PAGE = os.path.join(REPO, "docs", "coverage", "clickhouse-types.mdx")

PASS, SKIP, FAIL = "pass", "skip", "fail"

# Ordered cheapest to most real, which is the order they run in and the order
# the matrix reads left to right.
LEVELS = ("unit", "integration", "release")

# Go's integration pass selects its tests by name, because `go test -run` is
# the only selector that needs no build tag and no second package.
INTEGRATION_PREFIX = "TestIntegration"

# The closed vocabularies invariants.yml may use. A value outside one of them
# is a typo, and a typo must not reach the matrix as a missing cell.
FAMILIES = ("resilience", "checkpoint", "types", "lifecycle", "errors")

# What kind of claim an invariant makes, which is orthogonal to its family.
#
#   safety    nothing bad happens: never commit a position for rows the sink
#             did not take, never lose a batch a flush could not deliver.
#   liveness  something good eventually happens: buffered rows reach the sink,
#             a drain finishes, a stalled configuration fails at startup.
#
# The distinction is not academic. A sink that never flushes satisfies every
# safety invariant here -- keeps_batch holds vacuously if you never flush, and
# commit.after_flush holds if you never commit. Only a liveness claim says the
# pipeline does anything at all.
CLASSES = ("safety", "liveness")
KINDS = ("sink", "source", "handler", "pipeline")
# How an invariant collects evidence. Both are explicit markers: a test says
# what it proves. `named` used to mean "a test whose name matches the id",
# which was declared, never implemented, and would have been a third way to
# attribute the same claim.
VERIFIERS = ("harness", "typetable")

# What a type table may say a sink does with an Arrow type. Anything else is a
# typo, and a typo must not reach the matrix as a missing cell.
OUTCOMES = ("exact", "coerced", "unsupported")

PIPELINE = "pipeline"

# A pipeline configuration is an integration of the harness, though no
# constructor switch builds one. `constructed: false` says so, and the Kinds()
# agreement tests skip those entries.
INTEGRATION_KINDS = ("sink", "source", "handler", "pipeline")


def load_features():
    with open(REGISTRY) as fh:
        return yaml.safe_load(fh)["features"]


def load_invariants():
    with open(INVARIANTS) as fh:
        return yaml.safe_load(fh)["invariants"]


def load_integrations():
    with open(INTEGRATIONS) as fh:
        return yaml.safe_load(fh)["integrations"]


def load_lattice():
    with open(LATTICE) as fh:
        return yaml.safe_load(fh)["lattice"]


def validate_registries(invariants, integrations, features):
    """Every problem a human can fix, one line each. Empty when consistent.

    Checked before any test result is read. A registry typo would otherwise
    reach the matrix as a missing cell, and "missing" is the signal the matrix
    exists to carry: it must not be spendable on typos.
    """
    problems = []

    by_id = {}
    for inv in invariants:
        fid = inv.get("id")
        if fid in by_id:
            problems.append(f"invariants.yml: duplicate id {fid}")
        by_id[fid] = inv

        for key in ("family", "class", "applies_to", "claim", "verified_by",
                    "requires"):
            if key not in inv:
                problems.append(f"invariants.yml: {fid} has no {key}")
        if inv.get("family") not in FAMILIES:
            problems.append(
                f"invariants.yml: {fid} family {inv.get('family')!r} is not one of {FAMILIES}")
        if inv.get("class") not in CLASSES:
            problems.append(
                f"invariants.yml: {fid} class {inv.get('class')!r} is not one of {CLASSES}")
        if inv.get("applies_to") not in KINDS:
            problems.append(
                f"invariants.yml: {fid} applies_to {inv.get('applies_to')!r} is not one of {KINDS}")
        if inv.get("verified_by") not in VERIFIERS:
            problems.append(
                f"invariants.yml: {fid} verified_by {inv.get('verified_by')!r} is not one of {VERIFIERS}")
        for lvl in inv.get("requires", []):
            if lvl not in LEVELS:
                problems.append(
                    f"invariants.yml: {fid} requires {lvl!r}, which is not a level")

    feature_ids = {f["id"] for f in features}
    seen = set()
    for integ in integrations:
        iid = integ.get("id")
        if iid in seen:
            problems.append(f"integrations.yml: duplicate id {iid}")
        seen.add(iid)

        if integ.get("kind") not in INTEGRATION_KINDS:
            problems.append(
                f"integrations.yml: {iid} kind {integ.get('kind')!r} is not one of {INTEGRATION_KINDS}")
        # A test-only integration ships to nobody, so it has no feature and no
        # cells. It exists so the conformance harness's doubles have an id
        # that is not a real sink's.
        if not integ.get("test_only") and integ.get("feature") not in feature_ids:
            problems.append(
                f"integrations.yml: {iid} names feature {integ.get('feature')!r}, "
                "which features.yml does not declare")

        for ex in integ.get("exempt", []):
            inv = by_id.get(ex.get("invariant"))
            if inv is None:
                problems.append(
                    f"integrations.yml: {iid} is exempt from {ex.get('invariant')!r}, "
                    "which invariants.yml does not declare")
                continue
            if not ex.get("reason"):
                problems.append(
                    f"integrations.yml: {iid} exemption from {inv['id']} has no reason")
            # An exemption left as prose is an excuse. Two of them hid live
            # batch-loss bugs: sink.sqlcommand and sink.console were both
            # excused because nothing crosses a network, which is the argument
            # for skipping a retry ladder, not the invariant a ladder depends
            # on. A test must prove the premise.
            if not ex.get("proven_by"):
                problems.append(
                    f"integrations.yml: {iid} exemption from {inv['id']} has no "
                    "proven_by naming a test that proves the premise")
            if inv.get("applies_to") != integ.get("kind"):
                problems.append(
                    f"integrations.yml: {iid} is a {integ.get('kind')} but "
                    f"{inv['id']} applies_to {inv.get('applies_to')}")

    return problems


def render_types_page(lattice, integration):
    """The type mapping table, as the ClickHouse integration page publishes it.

    Keyed by DuckDB SQL type rather than by Arrow type, because a user writes a
    CAST and never sees an Arrow type. The Arrow key is the join and stays out
    of the output.

    Generated from the same declaration the type runner judges, so the page
    cannot claim something no test proved. That is the point: every row of the
    hand-written table it replaces was measured once, by hand, and nothing held
    it to the sink afterwards, so the first type change would have made the
    page wrong with no test to say so.
    """
    by_key = integration.get("types", {})
    supported, unsupported = [], []

    for entry in lattice:
        decl = by_key.get(entry["key"])
        if not decl:
            continue
        casts = ", ".join("`%s`" % d for d in entry["duckdb"])
        if decl["outcome"] == "unsupported":
            unsupported.append("- %s" % casts)
            continue
        cols = ", ".join("`%s`" % c for c in decl.get("columns", []))
        supported.append("| %s | %s | %s |" % (casts, cols, decl.get("rule", "")))

    out = [
        "<!-- Generated by scripts/coverage_matrix.py. Do not edit. -->",
        "<!-- Every row is proven by TestIntegrationSinkClickhouse_Types. -->",
        "",
        "## Type mapping {#type-mapping}",
        "",
        "sqlflow hands each batch to ClickHouse as rows built from the Arrow "
        "table DuckDB produces, so what matters is the type each SQL column "
        "ends up with. Cast in the handler SQL to control it.",
        "",
        "| DuckDB SQL type (cast to) | ClickHouse column types that accept it | Notes |",
        "| --- | --- | --- |",
    ]
    out += supported
    # The null rows are declared separately from the types because the answer
    # belongs to the destination column rather than to any Arrow type, and they
    # are the rows a reader is most likely to be surprised by: nothing raises,
    # and the absence is simply gone.
    nulls = integration.get("nulls", {})
    if nulls.get("default"):
        out.append("| `NULL` in any column | the column's own type | %s |"
                   % nulls["default"].get("rule", ""))
    if nulls.get("list_element"):
        out.append("| `NULL` inside a `LIST` | the element's own type | %s |"
                   % nulls["list_element"].get("rule", ""))

    out += [
        "",
        "Values are matched to columns **by name**, not by position.",
        "",
        "### Unsupported {#unsupported}",
        "",
        "These fail the batch with `user.sink.type_unsupported` rather than "
        "reaching the table. Cast or flatten them in the handler SQL.",
        "",
    ]
    out += unsupported
    out.append("")
    return "\n".join(out)


def validate_types(lattice, integrations):
    """Every problem in a type table, one line each. Empty when consistent.

    The lattice is closed, so a key outside it is a typo and a key inside it
    that a sink does not declare is a gap. Both are reported here rather than
    rendered as a missing cell: "missing" is the signal the matrix exists to
    carry, and it must not be spendable on a misspelling.

    A sink with no type table gets no lines at all. One sink has a table in
    this revision and five do not, and 29 lines each would bury the one that
    does. The absent tables are visible as missing cells, which is the right
    signal for a sink whose type behaviour nobody has written down.
    """
    problems = []
    keys = {entry["key"] for entry in lattice}

    for integ in integrations:
        types = integ.get("types")
        if not types:
            continue

        iid = integ["id"]
        for key, decl in sorted(types.items()):
            if key not in keys:
                problems.append(
                    f"integrations.yml: {iid} declares type {key!r}, "
                    "which lattice.yml does not list")
                continue

            outcome = decl.get("outcome")
            if outcome not in OUTCOMES:
                problems.append(
                    f"integrations.yml: {iid} type {key!r} outcome "
                    f"{outcome!r} is not one of {OUTCOMES}")
            # A coercion stated as an outcome and no rule is an excuse. The
            # rule is what the integration page publishes, and what a reader
            # needs to predict what their column will hold.
            if outcome == "coerced" and not decl.get("rule"):
                problems.append(
                    f"integrations.yml: {iid} type {key!r} is coerced with no rule")
            if outcome == "unsupported" and not decl.get("code"):
                problems.append(
                    f"integrations.yml: {iid} type {key!r} is unsupported with no code")
            if outcome in ("exact", "coerced") and not decl.get("columns"):
                problems.append(
                    f"integrations.yml: {iid} type {key!r} is {outcome} but names "
                    "no destination column type to write it to")

        for key in sorted(keys - set(types)):
            problems.append(
                f"integrations.yml: {iid} has a type table and declares no "
                f"outcome for {key!r}, which lattice.yml lists")

    return problems


# A plain marker names an extra feature. A structured one names an invariant
# and the integration it was proven on; the harness emits those, and a test
# name cannot carry two ids.
COVERS = re.compile(r"COVERS ([a-z0-9_.]+)(?:\s|$)")
COVERS_INVARIANT = re.compile(
    r"COVERS invariant=([a-z0-9_.]+) integration=([a-z0-9_.]+)")


def parse_go(path):
    """Read `go test -json` into ({test: outcome}, {test: [features]},
    {test: [(invariant, integration)]}).

    Markers come from coverage.Covers and coverage.Invariant, which write to
    the test log. `go test -json` carries that as an output event, so both are
    read from ordinary suite output with no plugin and no build tag.
    """
    results, covers, invariants = {}, {}, {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            name, action = event.get("Test"), event.get("Action")
            if not name:
                continue
            if action == "output":
                out = event.get("Output", "")
                structured = COVERS_INVARIANT.findall(out)
                if structured:
                    invariants.setdefault(name, []).extend(structured)
                    continue
                found = COVERS.findall(out)
                if found:
                    covers.setdefault(name, []).extend(found)
            # Subtests report their own outcome; the parent aggregates them.
            elif action == "pass":
                results.setdefault(name, PASS)
            elif action == "skip":
                results[name] = SKIP if results.get(name) != PASS else PASS
            elif action == "fail":
                results[name] = FAIL
    return results, covers, invariants


def parse_pytest(path):
    """Read the conftest report into ({test: outcome}, {test: [extras]}, {}).

    The third value is the invariant map, empty here: the harness is Go, and
    the release suite carries no structured markers. It is returned so both
    parsers have one shape.
    """
    with open(path) as fh:
        report = json.load(fh)

    results, covers = {}, {}
    for test in report.get("tests", []):
        name = test["nodeid"].rsplit("::", 1)[-1]
        results[name] = {
            "passed": PASS, "skipped": SKIP,
        }.get(test.get("outcome"), FAIL)
        if test.get("covers"):
            covers[name] = list(test["covers"])
    return results, covers, {}


def parent(name):
    """TestParent/case -> TestParent. A subtest is part of its parent."""
    return name.split("/", 1)[0]


def build(features, go_results, py_results, go_covers=None, py_covers=None,
          it_results=None, it_covers=None):
    """Attribute tests to features, by marker and only by marker.

    A test used to attribute to the feature whose id its name happened to
    prefix. That silently miscredited a test whose name merely started the same
    way, and silently credited nothing when a name drifted: #221 landed four
    tests that covered no feature and nothing said so until the matrix moved.

    A test now says what it covers. A subtest inherits its parent, because a
    subtest is part of that test rather than one of its own -- requiring a
    marker inside every t.Run would put one in 149 closures to repeat what the
    enclosing test already said.
    """
    known = {f["id"] for f in features}

    coverage = {f["id"]: {lvl: [] for lvl in LEVELS} for f in features}
    secondary = {f["id"]: {lvl: [] for lvl in LEVELS} for f in features}
    unmatched = {lvl: [] for lvl in LEVELS}
    unknown_markers = []

    for level, results, extras in (
        ("unit", go_results, go_covers or {}),
        ("integration", it_results or {}, it_covers or {}),
        ("release", py_results, py_covers or {}),
    ):
        for name, outcome in sorted(results.items()):
            # Deduped, order kept. A test can reach the same marker twice --
            # the conformance entry points emit one before the -short skip and
            # one when the harness finishes -- and counting both would put the
            # same name in a cell twice.
            claimed = extras.get(name) or extras.get(parent(name)) or []
            marks, seen = [], set()
            for feature_id in claimed:
                if feature_id not in seen:
                    seen.add(feature_id)
                    marks.append(feature_id)

            if not marks:
                # Report the parent once rather than every subtest under it:
                # naming the test is what a reader has to act on, and listing
                # its cases buries that line.
                if parent(name) not in unmatched[level]:
                    unmatched[level].append(parent(name))
                continue

            for feature_id in marks:
                if feature_id not in known:
                    unknown_markers.append((name, feature_id))
                    continue
                coverage[feature_id][level].append((name, outcome))
                # Every attribution is a marker now, so "secondary" means the
                # second and later features one test claims.
                if feature_id != marks[0]:
                    secondary[feature_id][level].append(name)

    return coverage, secondary, unmatched, unknown_markers


def build_invariants(invariants, integrations, results, evidence):
    """Attribute harness markers to (invariant, integration, level) cells.

    results:  {level: {test: outcome}}
    evidence: {level: {test: [(invariant, integration)]}}

    A marker is only ever emitted by a passing subtest, but the parent that
    ran it can still fail or be skipped afterwards, and that verdict is the
    one `go test` reports. The outcome recorded here is the test's, not the
    marker's presence.
    """
    # The integrations each invariant may be proven on. A marker outside this
    # set is a typo, and crediting it would invent a cell: a sink invariant
    # "proven on source.kafka" says nothing about either.
    kinds = {i["id"]: i["kind"] for i in integrations}
    allowed = {}
    for inv in invariants:
        allowed[inv["id"]] = {
            iid for iid, kind in kinds.items() if kind == inv["applies_to"]
        }

    cells = {}   # (invariant, integration, level) -> [(test, outcome)]
    unknown = []  # (test, invariant, integration)

    for level in LEVELS:
        # Insertion order, not sorted: snapshot_invariants sorts each cell, and
        # sorting twice hides which sort the output actually depends on.
        for test, pairs in evidence.get(level, {}).items():
            # A marker with no result means the event stream lost the verdict.
            # Treat that as a failure rather than as coverage.
            outcome = results.get(level, {}).get(test, FAIL)
            for inv, integ in pairs:
                if integ not in allowed.get(inv, set()):
                    unknown.append((test, inv, integ))
                    continue
                cells.setdefault((inv, integ, level), []).append((test, outcome))

    return {"cells": cells, "unknown": unknown}


def snapshot_invariants(invariants, integrations, built):
    """The invariant half of matrix.json.

    Same bare-name encoding as the feature half. An exempt cell carries its
    reason, so a reader never has to open a second file to learn why a cell is
    blank.
    """
    by_kind = {}
    for integ in integrations:
        # test_only integrations get no cells. Their markers are known, so the
        # harness's doubles are not reported as unknown, and they credit
        # nothing.
        if integ.get("test_only"):
            continue
        by_kind.setdefault(integ["kind"], []).append(integ)

    exemptions = {
        (ex["invariant"], integ["id"]): ex
        for integ in integrations
        for ex in integ.get("exempt", [])
    }

    out = {"invariants": [], "invariant_gaps": [], "unknown_invariant_markers": []}

    for inv in invariants:
        required = set(inv.get("requires", []))
        entry = {
            "id": inv["id"],
            "family": inv["family"],
            "applies_to": inv["applies_to"],
            # The YAML folds long claims onto several lines; one space between
            # words keeps the JSON diff stable when the wrapping changes.
            "claim": " ".join(inv["claim"].split()),
            "verified_by": inv["verified_by"],
            "class": inv["class"],
            "requires": sorted(required),
            "enforced": bool(inv.get("enforced")),
            "integrations": {},
        }
        if inv.get("tracked_by"):
            entry["tracked_by"] = inv["tracked_by"]
        if inv.get("violated_once"):
            entry["violated_once"] = list(inv["violated_once"])

        for integ in by_kind.get(inv["applies_to"], []):
            exemption = exemptions.get((inv["id"], integ["id"]))
            reason = exemption  # None when the integration must prove it
            levels = {}
            for level in LEVELS:
                if exemption is not None:
                    levels[level] = {
                        "status": "exempt",
                        "reason": exemption["reason"],
                        "proven_by": exemption.get("proven_by", ""),
                    }
                    continue

                # No not_required here, unlike the feature half. Every
                # non-exempt integration is expected to prove every invariant
                # of its kind; `requires` decides only whether the absence
                # fails the build. A cell with no evidence is missing, and
                # saying so is the whole point before anything is enforced.
                tests = sorted(built["cells"].get((inv["id"], integ["id"], level), []))
                state = status(tests)
                cell = {"status": state, "tests": [n for n, _ in tests]}
                for key, members in (
                    ("skipped", [n for n, o in tests if o == SKIP]),
                    ("failing", [n for n, o in tests if o == FAIL]),
                ):
                    if members:
                        cell[key] = members
                levels[level] = cell

                if level in required and state != "covered":
                    out["invariant_gaps"].append({
                        "invariant": inv["id"],
                        "integration": integ["id"],
                        "level": level,
                        "status": state,
                    })

            # An enforced invariant must be proven somewhere, and the level is
            # not the claim's business: ClickHouse and Kafka need a container,
            # console and sqlcommand fail in-process. Demanding a named level
            # would force a container on a sink that needs none, or accept a
            # fake for one that does.
            if inv.get("enforced") and reason is None:
                if not any(c["status"] == "covered" for c in levels.values()):
                    out["invariant_gaps"].append({
                        "invariant": inv["id"],
                        "integration": integ["id"],
                        "level": "any",
                        "status": cell_state(levels),
                    })

            entry["integrations"][integ["id"]] = levels

        out["invariants"].append(entry)

    # Sorted as tuples. Sorting dicts raises TypeError past one element, which
    # is what the feature half's unknown_markers does today.
    out["unknown_invariant_markers"] = [
        {"test": t, "invariant": i, "integration": g}
        for t, i, g in sorted(set(built["unknown"]))
    ]
    return out


def status(entries):
    """A level is covered only if something there actually ran and passed."""
    if not entries:
        return "missing"
    if any(o == FAIL for _, o in entries):
        return "failing"
    if all(o == SKIP for _, o in entries):
        return "skipped"
    return "covered"


MARK = {
    "covered": "✅",
    "skipped": "⚠️ skipped",
    "missing": "❌ **missing**",
    "failing": "🔥 failing",
    "not_required": "—",
}


def snapshot(features, coverage, secondary, unmatched, unknown_markers):
    """The machine-readable matrix.

    This is the artifact, and matrix.md is a view rendered from it. A diff
    against the committed copy is what makes a coverage change visible in
    review, and diffing JSON keeps that signal clean: reformatting the table
    or rewording a description cannot masquerade as a coverage change, and
    a coverage change cannot hide inside a reflowed table.

    Everything is sorted so the same tree always produces the same bytes.

    A test is recorded as a bare name, and the two facts that are almost never
    true -- it did not pass, it was attributed by a marker rather than by its
    name -- are named in `skipped`, `failing` and `by_marker` beside it. Those
    lists narrow `tests`; they never extend it. Empty ones are omitted.

    Recording the outcome and the attribution on every entry instead cost five
    lines and 124 bytes per test to carry about fifty bytes of fact, and the
    artifact is read far more often than it is written -- in review, and by
    agents answering questions about coverage. It came to 100KB, and 459 of its
    489 entries said nothing but `"outcome": "pass", "attribution": "name"`.
    """
    out = {"version": 3, "features": [], "gaps": [],
           "unattributed": {}, "unknown_markers": []}

    for feature in features:
        fid = feature["id"]
        required = set(feature.get("requires", []))
        entry = {
            "id": fid,
            "description": feature["description"],
            "requires": sorted(required),
            "levels": {},
        }

        for level in LEVELS:
            tests = sorted(coverage[fid][level])
            if level not in required and not tests:
                entry["levels"][level] = {"status": "not_required"}
                continue

            state = status(tests)
            secondaries = set(secondary[fid][level])
            names = [name for name, _ in tests]
            cell = {"status": state, "tests": names}
            for key, members in (
                ("skipped", [n for n, o in tests if o == SKIP]),
                ("failing", [n for n, o in tests if o == FAIL]),
                ("by_marker", [n for n in names if n in secondaries]),
            ):
                if members:
                    cell[key] = members
            entry["levels"][level] = cell

            if level in required and state != "covered":
                out["gaps"].append(
                    {"feature": fid, "level": level, "status": state})

        out["features"].append(entry)

    out["unattributed"] = {lvl: sorted(names) for lvl, names in unmatched.items()}
    out["unknown_markers"] = sorted(
        {"test": t, "feature": f} for t, f in unknown_markers
    ) if unknown_markers else []
    return out


def render(snap):
    """Render the human view. Everything here comes from the snapshot, so the
    markdown can never disagree with the JSON that gates the build."""
    lines = [
        "# Feature coverage matrix",
        "",
        "Generated from `docs/coverage/matrix.json` by `make coverage-matrix`.",
        "Do not edit by hand.",
        "",
        "Features are declared in `docs/coverage/features.yml`. A test attaches",
        "to one by saying so: `coverage.Covers(t, \"sink.clickhouse\")` in Go, or",
        "the `covers` marker in pytest. A test used to attach by the shape of",
        "its name, which credited a test whose name merely started the same way",
        "and credited nothing when a name drifted.",
        "",
        "Levels are derived from where a test ran, never declared, so they",
        "cannot drift. `unit` is `go test -short`, `integration` is the Go",
        "tests that need a real service, `release` is the image suite. A",
        "**skipped** test is not coverage at any of them: a skip that reads as",
        "a pass is how `sink.iceberg` shipped for months without ever being",
        "written to.",
        "",
        "The Tests column counts what attributes to the feature, so a thinly",
        "covered one is visible at a glance. `docs/coverage/matrix.json`",
        "names every one of them.",
        "",
        "| Feature | What it does | " + " | ".join(LEVELS) + " | Tests |",
        "| --- | --- | " + " | ".join("---" for _ in LEVELS) + " | --- |",
    ]

    for feature in snap["features"]:
        cells = " | ".join(MARK[feature["levels"][lvl]["status"]]
                           for lvl in LEVELS)
        count = sum(len(feature["levels"][lvl].get("tests", []))
                    for lvl in LEVELS)
        lines.append(
            f"| `{feature['id']}` | {feature['description']} | "
            f"{cells} | {count or '—'} |"
        )

    covered = sum(
        1 for f in snap["features"]
        if all(f["levels"][lvl]["status"] in ("covered", "not_required")
               for lvl in LEVELS)
    )
    lines += [
        "",
        f"**{len(snap['features'])} features declared. {covered} have at least one "
        f"passing test attributed at every level they require, so "
        f"{len(snap['gaps'])} gap(s).**",
        "",
        "That sentence counts attribution, not proof. A feature is green here when",
        "a test named for it ran and passed; it says nothing about whether the",
        "integration behind it keeps a batch it could not deliver, or commits",
        "offsets only after a flush. Those are invariants, they are counted",
        "separately below, and the two numbers are not interchangeable.",
        "",
    ]

    if snap.get("invariants"):
        tally, unwired = invariant_tally(snap)
        total = sum(tally.values())
        safety = sum(1 for i in snap["invariants"] if i["class"] == "safety")
        liveness = sum(1 for i in snap["invariants"] if i["class"] == "liveness")

        lines += [
            f"**{len(snap['invariants'])} invariants declared: {safety} safety "
            f"and {liveness} liveness. Of {total} (invariant, integration) "
            f"cells: {tally['covered']} proven, {tally['missing']} missing, "
            f"{tally['skipped']} skipped, {tally['failing']} failing, "
            f"{tally['exempt']} exempt. {len(snap['invariant_gaps'])} gap(s).**",
            "",
            "Safety says nothing bad happens. Liveness says something good",
            "eventually does, and the two are not interchangeable: a sink that",
            "never flushes satisfies every safety invariant on this page.",
            "`keeps_batch` holds if you never flush, and `commit.after_flush`",
            "holds if you never commit. Only a liveness claim says the pipeline",
            "does anything at all.",
            "",
        ]
        if unwired:
            lines += [
                f"A further {unwired} invariants carry no cell at all, so nothing",
                "can prove them. That is a declaration with no path to evidence,",
                "and it is worse than missing rather than better.",
                "",
            ]

        argument = strongest_argument(snap)
        if argument:
            feature, tests, invariant, proven, applicable = argument
            lines += [
                "## Why invariants, and not the test count",
                "",
                f"`{feature}` carries {tests} attributed tests, more than anything",
                f"else in the `{domain(feature)}` layer. `{invariant}`, an invariant",
                f"of that same layer, is proven on {proven} of the {applicable}",
                "integrations it applies to.",
                "",
                "Those two numbers are the argument. Tests accumulate around the",
                "code that was written; an invariant is the claim that code exists",
                "to uphold. A suite can exercise a retry ladder in every direction",
                "and never ask whether the sink underneath keeps the rows the",
                "ladder re-sends -- and if it does not, every one of those tests",
                "passes while the pipeline loses data. The feature table calls that",
                "covered. This one does not.",
                "",
            ]

    if snap["gaps"]:
        lines += ["## Gaps", "",
                  "These fail `make coverage-matrix`. There is no baseline: a gap",
                  "is closed by a test, or by the registry honestly no longer",
                  "requiring that level.", ""]
        for gap in snap["gaps"]:
            lines.append(
                f"- `{gap['feature']}` requires **{gap['level']}** coverage "
                f"and is *{gap['status']}*.")
        lines.append("")

    # Secondary attribution is visible on purpose. One test per feature is the
    # goal; a feature every test claims second is one no test is about, and
    # this is where that shows.
    by_marker = []
    for feature in snap["features"]:
        for level in LEVELS:
            cell = feature["levels"][level]
            tests = cell.get("tests", [])
            if tests and len(cell.get("by_marker", [])) == len(tests):
                by_marker.append((feature["id"], level, tests))
    if by_marker:
        lines += [
            "## Covered only by another test's marker",
            "",
            "Every test that covers these claims something else first. That is",
            "legitimate for a capability an end-to-end run proves in passing, and",
            "a smell for one that deserves a test of its own.",
            "",
        ]
        for fid, level, names in by_marker:
            lines.append(f"- `{fid}` ({level}) — via {', '.join(f'`{n}`' for n in names)}")
        lines.append("")

    for level in LEVELS:
        names = snap["unattributed"].get(level, [])
        if names:
            lines += [
                f"## Unattributed {level} tests ({len(names)})",
                "",
                "These carry no `coverage.Covers` marker, so they cover nothing.",
                "Add the marker, or add the feature to `features.yml` first.",
                "",
            ]
            lines += [f"- `{n}`" for n in names]
            lines.append("")

    if snap["unknown_markers"]:
        lines += ["## Markers naming an unknown feature", ""]
        for entry in snap["unknown_markers"]:
            lines.append(f"- `{entry['test']}` marks `{entry['feature']}`")
        lines.append("")

    return "\n".join(lines) + "\n"


# Level initials, so a cell fits: u = unit, i = integration, r = release.
LEVEL_INITIALS = {lvl: lvl[0] for lvl in LEVELS}


def cell_state(levels):
    """One status for one (invariant, integration) cell, worst first.

    Failing beats missing beats skipped beats covered, so a cell reports the
    worst thing that happened rather than the best.
    """
    if any(c["status"] == "exempt" for c in levels.values()):
        return "exempt"

    states = {cell["status"] for cell in levels.values()}
    if "failing" in states:
        return "failing"
    if "covered" in states:
        return "covered"
    if "skipped" in states:
        return "skipped"
    return "missing"


def invariant_cell(levels, required):
    """Render one integration's cell for one invariant.

    A covered cell names the levels that proved it, because "proven with a
    fake but never against the real thing" is the question this matrix exists
    to answer, and a bare tick cannot say it.
    """
    state = cell_state(levels)
    if state == "exempt":
        return "— exempt"
    if state == "failing":
        return "🔥 failing"
    if state == "skipped":
        return "⚠️ skipped"
    if state == "missing":
        return "❌ missing"

    states = {lvl: cell["status"] for lvl, cell in levels.items()}
    proven = "".join(LEVEL_INITIALS[lvl] for lvl in LEVELS
                     if states.get(lvl) == "covered")
    # A required level that is not covered is a gap, listed below. Say so here
    # too rather than showing a tick beside an unmet requirement.
    unmet = [lvl for lvl in required if states.get(lvl) != "covered"]
    if unmet:
        return f"⚠️ {proven}, {'+'.join(unmet)} missing"
    return f"✅ {proven}"


def describe_claim(inv):
    """The claim, plus the issue tracking it when the code does not hold it."""
    claim = inv["claim"]
    if inv.get("tracked_by"):
        claim += f" *(declared, tracked by {inv['tracked_by']})*"
    if inv.get("violated_once"):
        claim += f" *(violated once: {', '.join(inv['violated_once'])})*"
    return claim


def invariant_tally(snap):
    """Count every (invariant, integration) cell by state.

    A pipeline invariant carries one cell, so it counts once. `unwired` counts
    invariants that carry no cell at all, which should be none: an invariant
    nothing can prove is a declaration with no path to evidence.
    """
    tally = {s: 0 for s in ("covered", "missing", "skipped", "failing", "exempt")}
    unwired = 0
    for inv in snap["invariants"]:
        if not inv["integrations"]:
            unwired += 1
            continue
        for levels in inv["integrations"].values():
            tally[cell_state(levels)] += 1
    return tally, unwired


def domain(identifier):
    """sink.retry -> sink. The subsystem a feature or invariant belongs to."""
    return identifier.split(".", 1)[0]


def most_tested_feature(snap, within=None):
    """The feature carrying the most attributed tests, and how many.

    `within` restricts to one subsystem. Counted over features rather than
    integrations because the sharpest case is a cross-cutting one: sink.retry
    is not a sink anyone configures, and it accumulated more tests than any
    other part of the sink layer.
    """
    best = None
    for feature in snap["features"]:
        if within and domain(feature["id"]) != within:
            continue
        total = sum(len(feature["levels"][lvl].get("tests", [])) for lvl in LEVELS)
        if best is None or total > best[1]:
            best = (feature["id"], total)
    return best


def least_proven_invariant(snap, within=None):
    """The invariant proven on the fewest of the integrations it applies to.

    Exempt cells count as neither proof nor hole: an integration excused from
    an invariant is not evidence that the invariant is unproven.
    """
    worst = None
    for inv in snap["invariants"]:
        if within and domain(inv["id"]) != within:
            continue
        applicable = [levels for levels in inv["integrations"].values()
                      if cell_state(levels) != "exempt"]
        if not applicable:
            continue
        proven = sum(1 for levels in applicable if cell_state(levels) == "covered")
        share = proven / len(applicable)
        if worst is None or share < worst[3]:
            worst = (inv["id"], proven, len(applicable), share)
    return worst


def strongest_argument(snap):
    """A heavily tested feature paired with an unproven invariant of the same
    subsystem, as (feature, tests, invariant, proven, applicable).

    Same subsystem is what makes the pair an argument rather than a
    coincidence. A feature with many tests somewhere else in the engine says
    nothing about a sink invariant; a feature with many tests *in the sink
    layer* alongside a sink invariant nothing proves says exactly the thing
    this file exists to say.

    Generated rather than written down so it cannot go stale.
    """
    best = None
    for subsystem in sorted({domain(i["id"]) for i in snap["invariants"]}):
        feature = most_tested_feature(snap, within=subsystem)
        invariant = least_proven_invariant(snap, within=subsystem)
        if not feature or not invariant or feature[1] == 0:
            continue
        # Rank by tests: the more a subsystem is tested while an invariant of
        # its own goes unproven, the louder the point.
        if best is None or feature[1] > best[1]:
            best = (feature[0], feature[1], invariant[0], invariant[1], invariant[2])
    return best


def render_invariants(snap):
    """Render the invariant half. One table per family, one column per
    integration the family's invariants apply to."""
    lines = [
        "# Invariant matrix",
        "",
        "Generated from `docs/coverage/matrix.json` by `make coverage-matrix`.",
        "Do not edit by hand.",
        "",
        "Invariants are declared in `docs/coverage/invariants.yml`, integrations",
        "in `docs/coverage/integrations.yml`. A cell is proven by the conformance",
        "harness in `internal/conformance`, which emits a marker naming both",
        "ids -- a harness test's name says nothing, because the same code runs",
        "for every integration.",
        "",
        "A covered cell names the levels that proved it: `u` unit, `i`",
        "integration, `r` release. That is the question the matrix exists to",
        "answer -- proven with a fake, or against the real thing, or in the",
        "shipped image. **exempt** carries its reason in the JSON, and",
        "**missing** means no evidence. Nothing here fails the build until an",
        "invariant's `requires` is filled in, and none is yet.",
        "",
    ]

    # Grouped by class first. A liveness section that is entirely red sitting
    # beside a green safety one is the imbalance a reader has to see, and a
    # column would let it pass unnoticed.
    groups = []
    for inv in snap["invariants"]:
        key = (inv["class"], inv["family"])
        if key not in groups:
            groups.append(key)
    groups.sort(key=lambda k: (CLASSES.index(k[0]), FAMILIES.index(k[1])))

    for cls, family in groups:
        rows = [i for i in snap["invariants"]
                if i["class"] == cls and i["family"] == family]

        # Split on what the invariant applies to. A pipeline row in a table of
        # sink columns renders as a line of dots, which reads as "not
        # applicable" when the truth is "unproven".
        per_integration = [i for i in rows if i["applies_to"] != PIPELINE]
        per_pipeline = [i for i in rows if i["applies_to"] == PIPELINE]

        columns = []
        for inv in per_integration:
            for integ in inv["integrations"]:
                if integ not in columns:
                    columns.append(integ)

        lines += [f"## {cls.capitalize()} invariants: {family}", ""]

        if per_integration:
            lines.append("| Invariant | Claim | "
                         + " | ".join(f"`{c}`" for c in columns) + " |")
            lines.append("| --- | --- | "
                         + " | ".join("---" for _ in columns) + " |")
            for inv in per_integration:
                cells = " | ".join(
                    invariant_cell(inv["integrations"][c], inv["requires"])
                    if c in inv["integrations"] else "·"
                    for c in columns)
                lines.append(f"| `{inv['id']}` | {describe_claim(inv)} | {cells} |")
            lines.append("")

        if per_pipeline:
            lines += [
                f"These {family} invariants are properties of the consume loop",
                "rather than of anything a config file names. The columns are",
                "its configurations, and `internal/conformance` runs each one",
                "through every path that reaches a batch: the batch filling, the",
                "flush interval elapsing, the source closing, and a cancel that",
                "drains. An invariant holds only if it holds on all four.",
                "",
            ]
            pipeline_columns = []
            for inv in per_pipeline:
                for integ in inv["integrations"]:
                    if integ not in pipeline_columns:
                        pipeline_columns.append(integ)

            lines.append("| Invariant | Claim | "
                         + " | ".join(f"`{c}`" for c in pipeline_columns) + " |")
            lines.append("| --- | --- | "
                         + " | ".join("---" for _ in pipeline_columns) + " |")
            for inv in per_pipeline:
                cells = " | ".join(
                    invariant_cell(inv["integrations"][c], inv["requires"])
                    if c in inv["integrations"] else "·"
                    for c in pipeline_columns)
                lines.append(f"| `{inv['id']}` | {describe_claim(inv)} | {cells} |")
            lines.append("")

    if snap["invariant_gaps"]:
        lines += ["## Invariant gaps", "",
                  "These fail `make coverage-check`.", ""]
        for gap in snap["invariant_gaps"]:
            lines.append(
                f"- `{gap['invariant']}` on `{gap['integration']}` requires "
                f"**{gap['level']}** and is *{gap['status']}*.")
        lines.append("")

    if snap["unknown_invariant_markers"]:
        lines += ["## Markers naming an unknown invariant or integration", ""]
        for entry in snap["unknown_invariant_markers"]:
            lines.append(f"- `{entry['test']}` marks `{entry['invariant']}` "
                         f"on `{entry['integration']}`")
        lines.append("")

    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--go", help="go test -short -json output")
    ap.add_argument("--go-integration",
                    help="go test -json output from the integration pass")
    ap.add_argument("--pytest", help="pytest result json from the conftest hook")
    ap.add_argument("--write", action="store_true",
                    help="write matrix.json and matrix.md")
    ap.add_argument("--check", action="store_true", help="exit non-zero on gaps")
    args = ap.parse_args()

    features = load_features()
    invariants = load_invariants()
    integrations = load_integrations()

    # Before any test result is read: a registry typo would otherwise reach
    # the matrix as a missing cell.
    problems = validate_registries(invariants, integrations, features)
    problems += validate_types(load_lattice(), integrations)
    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 2

    go_results, go_covers, go_invariants = (
        parse_go(args.go) if args.go and os.path.exists(args.go) else ({}, {}, {}))
    it_results, it_covers, it_invariants = (
        parse_go(args.go_integration)
        if args.go_integration and os.path.exists(args.go_integration)
        else ({}, {}, {}))
    py_results, py_covers, py_invariants = (
        parse_pytest(args.pytest)
        if args.pytest and os.path.exists(args.pytest) else ({}, {}, {}))

    coverage, secondary, unmatched, unknown = build(
        features, go_results, py_results, go_covers, py_covers,
        it_results, it_covers)
    snap = snapshot(features, coverage, secondary, unmatched, unknown)

    built = build_invariants(
        invariants, integrations,
        {"unit": go_results, "integration": it_results, "release": py_results},
        {"unit": go_invariants, "integration": it_invariants,
         "release": py_invariants})
    snap.update(snapshot_invariants(invariants, integrations, built))

    rendered = render(snap) + "\n" + render_invariants(snap)

    # The published page is generated from the registries alone, not from any
    # test report: it says what the declaration claims, and the type runner is
    # what holds the declaration to the sink.
    lattice = load_lattice()
    clickhouse = next(
        (i for i in integrations if i["id"] == "sink.clickhouse"), None)
    types_page = render_types_page(lattice, clickhouse) if clickhouse else ""

    if args.write:
        os.makedirs(os.path.dirname(MATRIX_JSON), exist_ok=True)
        with open(MATRIX_JSON, "w") as fh:
            json.dump(snap, fh, indent=2, sort_keys=False)
            fh.write("\n")
        with open(MATRIX_MD, "w") as fh:
            fh.write(rendered)
        if types_page:
            with open(TYPES_PAGE, "w") as fh:
                fh.write(types_page)
        print(f"wrote {os.path.relpath(MATRIX_JSON, REPO)}, "
              f"{os.path.relpath(MATRIX_MD, REPO)} "
              f"and {os.path.relpath(TYPES_PAGE, REPO)}")
    else:
        print(rendered)

    gaps = snap["gaps"] + snap["invariant_gaps"]
    if args.check and gaps:
        print(f"\n{len(gaps)} coverage gap(s):", file=sys.stderr)
        for gap in snap["gaps"]:
            print(f"  {gap['feature']}: {gap['level']} is {gap['status']}",
                  file=sys.stderr)
        for gap in snap["invariant_gaps"]:
            print(f"  {gap['invariant']} on {gap['integration']}: "
                  f"{gap['level']} is {gap['status']}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
