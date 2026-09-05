#!/usr/bin/env python3
"""Build the feature coverage matrix from test suite output.

Two axes, deliberately independent:

  Feature  what sqlflow does, e.g. sink.iceberg. Declared in
           docs/coverage/features.yml. Never encodes a test level.
  Level    where the test ran: `unit` for go test, `release` for the image
           suite. Derived from which file the result came out of, never
           declared, so it cannot drift from reality.

Keeping them separate is what lets the matrix answer the question that went
unanswered for months: which features are covered by a unit test but never
proven against the shipped image.

A skipped test does not cover its feature. It reports as SKIP, because a skip
that reads as coverage is exactly how sink.iceberg shipped untested.

Usage:
    coverage_matrix.py --go go.json --pytest pytest.json --write
    coverage_matrix.py --go go.json --pytest pytest.json --check

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
# The JSON is the artifact CI diffs; the markdown is a view rendered from it.
MATRIX_JSON = os.path.join(REPO, "docs", "coverage", "matrix.json")
MATRIX_MD = os.path.join(REPO, "docs", "coverage", "matrix.md")

PASS, SKIP, FAIL = "pass", "skip", "fail"


def load_features():
    with open(REGISTRY) as fh:
        return yaml.safe_load(fh)["features"]


def go_prefix(feature_id):
    """sink.clickhouse -> TestSinkClickhouse"""
    parts = re.split(r"[._]", feature_id)
    return "Test" + "".join(p.capitalize() for p in parts)


def py_prefix(feature_id):
    """sink.clickhouse -> test_sink_clickhouse"""
    return "test_" + feature_id.replace(".", "_")


def match(name, prefixes):
    """Longest matching prefix wins, so sink.iceberg does not swallow
    sink.iceberg_merge."""
    best = None
    for feature_id, prefix in prefixes.items():
        if name.startswith(prefix):
            if best is None or len(prefix) > len(prefixes[best]):
                best = feature_id
    return best


COVERS = re.compile(r"COVERS ([a-z0-9_.]+)")


def parse_go(path):
    """Read `go test -json` into ({test: outcome}, {test: [extra features]}).

    Extras come from coverage.Covers, which writes a COVERS line to the test
    log. `go test -json` carries that as an output event, so the marker is
    read from ordinary suite output with no plugin and no build tag.
    """
    results, covers = {}, {}
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
                found = COVERS.findall(event.get("Output", ""))
                if found:
                    covers.setdefault(name, []).extend(found)
            # Subtests report their own outcome; the parent aggregates them.
            elif action == "pass":
                results.setdefault(name, PASS)
            elif action == "skip":
                results[name] = SKIP if results.get(name) != PASS else PASS
            elif action == "fail":
                results[name] = FAIL
    return results, covers


def parse_pytest(path):
    """Read the conftest report into ({test: outcome}, {test: [extras]})."""
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
    return results, covers


def build(features, go_results, py_results, go_covers=None, py_covers=None):
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

    coverage = {f["id"]: {"unit": [], "release": []} for f in features}
    secondary = {f["id"]: {"unit": [], "release": []} for f in features}
    unmatched = {"unit": [], "release": []}
    unknown_markers = []

    for level, results, prefixes, extras in (
        ("unit", go_results, go_prefixes, go_covers or {}),
        ("release", py_results, py_prefixes, py_covers or {}),
    ):
        for name, outcome in sorted(results.items()):
            feature_id = match(name, prefixes)
            if feature_id:
                coverage[feature_id][level].append((name, outcome))
            else:
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
    """
    out = {"version": 1, "features": [], "gaps": [],
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

        for level in ("unit", "release"):
            tests = sorted(coverage[fid][level])
            if level not in required and not tests:
                entry["levels"][level] = {"status": "not_required", "tests": []}
                continue

            state = status(tests)
            secondaries = set(secondary[fid][level])
            entry["levels"][level] = {
                "status": state,
                "tests": [
                    {"name": name, "outcome": outcome,
                     "attribution": "marker" if name in secondaries else "name"}
                    for name, outcome in tests
                ],
            }
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
        "cannot drift. A **skipped** test is not coverage: a skip that reads as",
        "a pass is how `sink.iceberg` shipped for months without ever being",
        "written to.",
        "",
        "| Feature | What it does | unit | release | Tests |",
        "| --- | --- | --- | --- | --- |",
    ]

    for feature in snap["features"]:
        cells = {lvl: MARK[feature["levels"][lvl]["status"]]
                 for lvl in ("unit", "release")}
        names = [t["name"] for lvl in ("unit", "release")
                 for t in feature["levels"][lvl]["tests"]]
        shown = ", ".join(f"`{n}`" for n in names[:3])
        if len(names) > 3:
            shown += f" +{len(names) - 3} more"
        lines.append(
            f"| `{feature['id']}` | {feature['description']} | "
            f"{cells['unit']} | {cells['release']} | {shown or '—'} |"
        )

    covered = sum(
        1 for f in snap["features"]
        if all(f["levels"][lvl]["status"] in ("covered", "not_required")
               for lvl in ("unit", "release"))
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
        for level in ("unit", "release"):
            tests = feature["levels"][level]["tests"]
            if tests and all(t["attribution"] == "marker" for t in tests):
                by_marker.append((feature["id"], level, [t["name"] for t in tests]))
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

    for level in ("unit", "release"):
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--go", help="go test -json output")
    ap.add_argument("--pytest", help="pytest result json from the conftest hook")
    ap.add_argument("--write", action="store_true",
                    help="write matrix.json and matrix.md")
    ap.add_argument("--check", action="store_true", help="exit non-zero on gaps")
    args = ap.parse_args()

    features = load_features()
    go_results, go_covers = (
        parse_go(args.go) if args.go and os.path.exists(args.go) else ({}, {}))
    py_results, py_covers = (
        parse_pytest(args.pytest)
        if args.pytest and os.path.exists(args.pytest) else ({}, {}))

    coverage, secondary, unmatched, unknown = build(
        features, go_results, py_results, go_covers, py_covers)
    snap = snapshot(features, coverage, secondary, unmatched, unknown)
    rendered = render(snap)

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

    if args.check and snap["gaps"]:
        print(f"\n{len(snap['gaps'])} coverage gap(s):", file=sys.stderr)
        for gap in snap["gaps"]:
            print(f"  {gap['feature']}: {gap['level']} is {gap['status']}",
                  file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
