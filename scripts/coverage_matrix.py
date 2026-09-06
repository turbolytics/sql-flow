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
# The JSON is the artifact CI diffs; the markdown is a view rendered from it.
MATRIX_JSON = os.path.join(REPO, "docs", "coverage", "matrix.json")
MATRIX_MD = os.path.join(REPO, "docs", "coverage", "matrix.md")

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
KINDS = ("sink", "source", "handler", "pipeline")
VERIFIERS = ("harness", "typetable", "named")

# pipeline invariants attach to core, not to a constructor case, so no entry
# in integrations.yml carries that kind.
INTEGRATION_KINDS = ("sink", "source", "handler")


def load_features():
    with open(REGISTRY) as fh:
        return yaml.safe_load(fh)["features"]


def load_invariants():
    with open(INVARIANTS) as fh:
        return yaml.safe_load(fh)["invariants"]


def load_integrations():
    with open(INTEGRATIONS) as fh:
        return yaml.safe_load(fh)["integrations"]


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

        for key in ("family", "applies_to", "claim", "verified_by", "requires"):
            if key not in inv:
                problems.append(f"invariants.yml: {fid} has no {key}")
        if inv.get("family") not in FAMILIES:
            problems.append(
                f"invariants.yml: {fid} family {inv.get('family')!r} is not one of {FAMILIES}")
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
        if integ.get("feature") not in feature_ids:
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
            if inv.get("applies_to") != integ.get("kind"):
                problems.append(
                    f"integrations.yml: {iid} is a {integ.get('kind')} but "
                    f"{inv['id']} applies_to {inv.get('applies_to')}")

    return problems


def go_prefix(feature_id):
    """sink.clickhouse -> TestSinkClickhouse"""
    parts = re.split(r"[._]", feature_id)
    return "Test" + "".join(p.capitalize() for p in parts)


def py_prefix(feature_id):
    """sink.clickhouse -> test_sink_clickhouse"""
    return "test_" + feature_id.replace(".", "_")


def strip_level_prefix(name):
    """TestIntegrationSourceKafka_Commits -> TestSourceKafka_Commits

    The prefix says which pass runs the test. It must not also say which
    feature the test covers, or every integration test would attribute to
    nothing and land in the unattributed list.
    """
    if name.startswith(INTEGRATION_PREFIX):
        return "Test" + name[len(INTEGRATION_PREFIX):]
    return name


def match(name, prefixes):
    """Longest matching prefix wins, so sink.iceberg does not swallow
    sink.iceberg_merge."""
    best = None
    for feature_id, prefix in prefixes.items():
        if name.startswith(prefix):
            if best is None or len(prefix) > len(prefixes[best]):
                best = feature_id
    return best


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


def build(features, go_results, py_results, go_covers=None, py_covers=None,
          it_results=None, it_covers=None):
    """Attribute tests to features.

    The name is the primary attribution and stays the cheap default: rename a
    test and it is attributed, with no import and no marker. Markers add the
    extra features an end-to-end test genuinely proves, which a single name
    cannot express. A feature covered only by markers has no test of its own,
    and the secondary-attribution section below says so.
    """
    known = {f["id"] for f in features}
    go_prefixes = {f["id"]: go_prefix(f["id"]) for f in features}
    py_prefixes = {f["id"]: py_prefix(f["id"]) for f in features}

    coverage = {f["id"]: {lvl: [] for lvl in LEVELS} for f in features}
    secondary = {f["id"]: {lvl: [] for lvl in LEVELS} for f in features}
    unmatched = {lvl: [] for lvl in LEVELS}
    unknown_markers = []

    for level, results, prefixes, extras in (
        ("unit", go_results, go_prefixes, go_covers or {}),
        ("integration", it_results or {}, go_prefixes, it_covers or {}),
        ("release", py_results, py_prefixes, py_covers or {}),
    ):
        for name, outcome in sorted(results.items()):
            feature_id = match(strip_level_prefix(name), prefixes)
            if feature_id:
                coverage[feature_id][level].append((name, outcome))
            elif not extras.get(name):
                # A test carrying a marker is attributed by it. Listing it here
                # would tell the reader to rename a test that already says what
                # it covers, and the conformance harness attributes only by
                # marker: its runner's name matches no feature at all.
                unmatched[level].append(name)

            for extra in extras.get(name, []):
                if extra not in known:
                    unknown_markers.append((name, extra))
                    continue
                if extra == feature_id:
                    continue
                coverage[extra][level].append((name, outcome))
                secondary[extra][level].append(name)

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
    known_inv = {i["id"] for i in invariants}
    known_integ = {i["id"] for i in integrations}

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
                if inv not in known_inv or integ not in known_integ:
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
        by_kind.setdefault(integ["kind"], []).append(integ)

    exemptions = {
        (ex["invariant"], integ["id"]): ex["reason"]
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
            "requires": sorted(required),
            "integrations": {},
        }
        if inv.get("tracked_by"):
            entry["tracked_by"] = inv["tracked_by"]
        if inv.get("violated_once"):
            entry["violated_once"] = list(inv["violated_once"])

        # A pipeline invariant attaches to core, so it has no subjects.
        for integ in by_kind.get(inv["applies_to"], []):
            reason = exemptions.get((inv["id"], integ["id"]))
            levels = {}
            for level in LEVELS:
                if reason is not None:
                    levels[level] = {"status": "exempt", "reason": reason}
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
        "to one by name -- `sink.clickhouse` is covered by `TestSinkClickhouse*`",
        "or `test_sink_clickhouse*` -- and that is the cheap default: rename a",
        "test and it is attributed, with no import and no marker.",
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
        f"**{len(snap['features'])} features declared, {covered} fully covered, "
        f"{len(snap['gaps'])} gap(s).**",
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
    # goal; a feature reached only through another test's marker has no test of
    # its own, and this is where that shows.
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
            "These features have no test named for them. That is legitimate for a",
            "capability an end-to-end run proves in passing, and a smell for one",
            "that deserves its own test.",
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
                "These match no declared feature. Either rename them to the",
                "convention, or add the feature to `features.yml`.",
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


INV_MARK = {
    "covered": "✅",
    "skipped": "⚠️ skipped",
    "missing": "❌ missing",
    "failing": "🔥 failing",
    "exempt": "— exempt",
}

# Worst first. A cell shows the weakest status across the levels it is judged
# on, so a green row means green everywhere it was asked.
STATUS_RANK = ("failing", "missing", "skipped", "covered", "exempt")


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
        "**exempt** carries its reason in the JSON. **missing** means no",
        "evidence. Nothing here fails the build until an invariant's `requires`",
        "is filled in, and none is yet.",
        "",
    ]

    def worst(levels, required):
        judged = required or list(levels)
        states = [levels[lvl]["status"] for lvl in judged if lvl in levels]
        return min(states, key=STATUS_RANK.index) if states else "missing"

    families = []
    for inv in snap["invariants"]:
        if inv["family"] not in families:
            families.append(inv["family"])

    for family in families:
        rows = [i for i in snap["invariants"] if i["family"] == family]
        columns = []
        for inv in rows:
            for integ in inv["integrations"]:
                if integ not in columns:
                    columns.append(integ)

        lines += [f"## Invariants: {family}", ""]
        if columns:
            lines.append("| Invariant | Claim | "
                         + " | ".join(f"`{c}`" for c in columns) + " |")
            lines.append("| --- | --- | "
                         + " | ".join("---" for _ in columns) + " |")
        else:
            # pipeline invariants have no per-integration column.
            lines.append("| Invariant | Claim | Verified by |")
            lines.append("| --- | --- | --- |")

        for inv in rows:
            claim = inv["claim"]
            if inv.get("tracked_by"):
                claim += f" *(declared, tracked by {inv['tracked_by']})*"
            if columns:
                cells = " | ".join(
                    INV_MARK[worst(inv["integrations"][c], inv["requires"])]
                    if c in inv["integrations"] else "·"
                    for c in columns)
                lines.append(f"| `{inv['id']}` | {claim} | {cells} |")
            else:
                lines.append(f"| `{inv['id']}` | {claim} | {inv['verified_by']} |")
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

    if args.write:
        os.makedirs(os.path.dirname(MATRIX_JSON), exist_ok=True)
        with open(MATRIX_JSON, "w") as fh:
            json.dump(snap, fh, indent=2, sort_keys=False)
            fh.write("\n")
        with open(MATRIX_MD, "w") as fh:
            fh.write(rendered)
        print(f"wrote {os.path.relpath(MATRIX_JSON, REPO)} "
              f"and {os.path.relpath(MATRIX_MD, REPO)}")
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
