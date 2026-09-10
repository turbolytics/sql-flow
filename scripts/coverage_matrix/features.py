"""The feature axis: what sqlflow does, and what covers it.

A test attaches to a feature by saying so. It used to attach by the shape
of its name, which credited a test whose name merely started the same way
and credited nothing when a name drifted.
"""

from .registries import FAIL, LEVELS, SKIP
from .suites import parent


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


def feature_gaps(entries):
    """A required level that is not covered, for every feature entry.

    Reads statuses and `requires` only, so the page can compute the same
    gaps from the committed status files that the gate computes from the
    test reports.
    """
    gaps = []
    for entry in entries:
        for level in LEVELS:
            state = entry["levels"][level]["status"]
            if level in entry["requires"] and state != "covered":
                gaps.append({"feature": entry["id"], "level": level, "status": state})
    return gaps


def status(entries):
    """A level is covered only if something there actually ran and passed."""
    if not entries:
        return "missing"
    if any(o == FAIL for _, o in entries):
        return "failing"
    if all(o == SKIP for _, o in entries):
        return "skipped"
    return "covered"


def snapshot(features, coverage, secondary, unmatched, unknown_markers):
    """The machine-readable matrix, written to .coverage/matrix.json.

    Nothing commits this. It is what the gate judges gaps from, what
    status_from_snapshot projects, and where the report's counts point for the
    names behind them. What review sees is the status directory this projects
    to: a diff of statuses cannot be buried under a test that was renamed.

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

        out["features"].append(entry)

    out["gaps"] = feature_gaps(out["features"])
    out["unattributed"] = {lvl: sorted(names) for lvl, names in unmatched.items()}
    out["unknown_markers"] = sorted(
        {"test": t, "feature": f} for t, f in unknown_markers
    ) if unknown_markers else []
    return out
