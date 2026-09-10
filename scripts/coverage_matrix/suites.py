"""What the suites wrote, read back.

Markers come from coverage.Covers and coverage.Invariant, which write to
the test log. `go test -json` carries that as an output event, so both are
read from ordinary suite output with no plugin and no build tag.
"""

import json
import re

from .registries import FAIL, PASS, SKIP


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
