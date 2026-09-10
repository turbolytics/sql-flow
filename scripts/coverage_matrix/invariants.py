"""The invariant axis: what every integration must hold.

A feature attributes by marker naming one id. An invariant cannot: the
conformance harness runs the same code for every integration, so the
marker carries both ids and only it knows which run this was.
"""

from .features import status
from .registries import FAIL, LEVELS, SKIP


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
    """The invariant half of the snapshot.

    Same bare-name encoding as the feature half. An exempt cell carries its
    reason, so a reader of the snapshot never has to open the registry to
    learn why a cell is blank.
    """
    kinds = by_kind(integrations)
    exemptions = exemptions_of(integrations)

    out = {"invariants": [], "invariant_gaps": [], "unknown_invariant_markers": []}

    for inv in invariants:
        entry = declare(inv)

        for integ in kinds.get(inv["applies_to"], []):
            exemption = exemptions.get((inv["id"], integ["id"]))
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

            entry["integrations"][integ["id"]] = levels

        out["invariants"].append(entry)

    out["invariant_gaps"] = invariant_gaps(out["invariants"])

    # Sorted as tuples. Sorting dicts raises TypeError past one element, which
    # is what the feature half's unknown_markers does today.
    out["unknown_invariant_markers"] = [
        {"test": t, "invariant": i, "integration": g}
        for t, i, g in sorted(set(built["unknown"]))
    ]
    return out


def invariant_gaps(entries):
    """A required level that is not covered, and an enforced invariant with
    no covered level at all, for every non-exempt cell.

    An enforced invariant must be proven somewhere, and the level is not the
    claim's business: ClickHouse and Kafka need a container, console and
    sqlcommand fail in-process. Demanding a named level would force a
    container on a sink that needs none, or accept a fake for one that does.
    """
    gaps = []
    for entry in entries:
        required = set(entry["requires"])
        for iid, levels in entry["integrations"].items():
            if cell_state(levels) == "exempt":
                continue
            for level in LEVELS:
                state = levels[level]["status"]
                if level in required and state != "covered":
                    gaps.append({"invariant": entry["id"], "integration": iid,
                                 "level": level, "status": state})
            if entry.get("enforced") and not any(
                    c["status"] == "covered" for c in levels.values()):
                gaps.append({"invariant": entry["id"], "integration": iid,
                             "level": "any", "status": cell_state(levels)})
    return gaps


def declare(inv):
    """The declaration half of an invariant entry, without its cells."""
    entry = {
        "id": inv["id"],
        "family": inv["family"],
        "applies_to": inv["applies_to"],
        # The YAML folds long claims onto several lines; one space between
        # words keeps the output stable when the wrapping changes.
        "claim": " ".join(inv["claim"].split()),
        "verified_by": inv["verified_by"],
        "class": inv["class"],
        "requires": sorted(inv.get("requires", [])),
        "enforced": bool(inv.get("enforced")),
        "integrations": {},
    }
    if inv.get("tracked_by"):
        entry["tracked_by"] = inv["tracked_by"]
    if inv.get("violated_once"):
        entry["violated_once"] = list(inv["violated_once"])
    return entry


def exemptions_of(integrations):
    """(invariant, integration) -> the exemption the registry declares."""
    return {
        (ex["invariant"], integ["id"]): ex
        for integ in integrations
        for ex in integ.get("exempt", [])
    }


def by_kind(integrations):
    """kind -> the integrations that get cells. test_only ones get none: their
    markers are known, so the harness's doubles are not reported as unknown,
    and they credit nothing."""
    out = {}
    for integ in integrations:
        if integ.get("test_only"):
            continue
        out.setdefault(integ["kind"], []).append(integ)
    return out


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
