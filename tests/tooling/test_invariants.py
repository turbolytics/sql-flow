"""Tests for coverage_matrix.invariants."""

import json

from coverage_matrix.invariants import (
    cell_state, declare, invariant_gaps, invariant_tally)
from coverage_matrix.registries import FAIL, LEVELS, PASS, SKIP
from samples import *  # noqa: F401,F403


def test_invariant_gaps_skips_an_exempt_cell():
    entries = [{
        "id": "sink.flush.keeps_batch", "requires": ["unit"], "enforced": True,
        "integrations": {
            "sink.console": {lvl: {"status": "exempt"} for lvl in LEVELS},
            "sink.clickhouse": {lvl: {"status": "missing"} for lvl in LEVELS},
        },
    }]
    gaps = invariant_gaps(entries)
    assert {"invariant": "sink.flush.keeps_batch", "integration": "sink.clickhouse",
            "level": "unit", "status": "missing"} in gaps
    assert {"invariant": "sink.flush.keeps_batch", "integration": "sink.clickhouse",
            "level": "any", "status": "missing"} in gaps
    assert not any(g["integration"] == "sink.console" for g in gaps)


def test_declare_carries_the_declaration_and_nothing_else():
    entry = declare(dict(INVARIANTS[0], claim="a claim\nwrapped", tracked_by="#1"))
    assert entry["claim"] == "a claim wrapped"
    assert entry["tracked_by"] == "#1"
    assert entry["integrations"] == {}
    assert "violated_once" not in entry


def test_a_passing_harness_case_covers_the_cell():
    name = "TestIntegrationSinkClickhouse_Conformance/keeps_batch"
    s = inv_snap(
        evidence={"integration": {name: [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {name: PASS}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "integration")
    assert c["status"] == "covered"
    assert c["tests"] == [name]


def test_a_marker_from_a_failing_test_is_not_coverage():
    """The harness emits the marker only on pass, but the parent that ran the
    subtest can still fail afterwards, and that verdict is the reported one."""
    s = inv_snap(
        evidence={"integration": {"TestX": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {"TestX": FAIL}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "integration")["status"] == "failing"


def test_a_marker_from_a_skipped_test_is_not_coverage():
    s = inv_snap(
        evidence={"integration": {"TestX": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {"TestX": SKIP}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "integration")["status"] == "skipped"


def test_an_integration_with_no_evidence_is_missing():
    s = inv_snap()
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "integration")["status"] == "missing"


def test_an_exempt_integration_renders_exempt_with_its_reason():
    s = inv_snap()
    c = cell(s, "sink.flush.keeps_batch", "sink.console", "integration")
    assert c["status"] == "exempt"
    assert c["reason"] == "stdout"


def test_an_invariant_applies_only_to_its_kind():
    """A source has no cell under a sink invariant at all."""
    s = inv_snap()
    inv = next(i for i in s["invariants"] if i["id"] == "sink.flush.keeps_batch")
    assert "source.kafka" not in inv["integrations"]
    assert set(inv["integrations"]) == {"sink.clickhouse", "sink.console"}


def test_an_unrequired_level_is_not_a_gap():
    s = inv_snap()
    assert s["invariant_gaps"] == []


def test_a_required_level_with_no_evidence_is_a_gap():
    required = [dict(INVARIANTS[0], requires=["integration"])]
    s = inv_snap(invariants=required)
    assert {"invariant": "sink.flush.keeps_batch", "integration": "sink.clickhouse",
            "level": "integration", "status": "missing"} in s["invariant_gaps"]
    # console is exempt, so it is not a gap even at a required level.
    assert not any(g["integration"] == "sink.console" for g in s["invariant_gaps"])


def test_a_marker_naming_an_unknown_invariant_or_integration_is_reported():
    s = inv_snap(
        evidence={"integration": {"TestX": [("sink.flush.bogus", "sink.clickhouse"),
                                            ("sink.flush.keeps_batch", "sink.bogus")]}},
        results={"integration": {"TestX": PASS}},
    )
    assert {"test": "TestX", "invariant": "sink.flush.bogus",
            "integration": "sink.clickhouse"} in s["unknown_invariant_markers"]
    assert {"test": "TestX", "invariant": "sink.flush.keeps_batch",
            "integration": "sink.bogus"} in s["unknown_invariant_markers"]


def test_two_unknown_invariant_markers_do_not_crash_the_snapshot():
    """Guard against the sorted()-over-dicts crash the feature snapshot has:
    sort by tuple, never by dict."""
    s = inv_snap(
        evidence={"integration": {"TestX": [("a.b.c", "sink.clickhouse")],
                                  "TestY": [("d.e.f", "sink.clickhouse")]}},
        results={"integration": {"TestX": PASS, "TestY": PASS}},
    )
    assert len(s["unknown_invariant_markers"]) == 2


def test_a_tracked_invariant_renders_as_declared_unenforced():
    tracked = [dict(INVARIANTS[0], tracked_by="#183")]
    s = inv_snap(invariants=tracked)
    inv = next(i for i in s["invariants"] if i["id"] == "sink.flush.keeps_batch")
    assert inv["tracked_by"] == "#183"


def test_build_keys_a_cell_by_invariant_integration_and_level():
    b = built(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert b["cells"] == {
        ("sink.flush.keeps_batch", "sink.clickhouse", "unit"): [("TestA", PASS)]
    }


def test_build_records_the_same_invariant_at_each_level_separately():
    """unit and integration are different evidence for the same claim, and the
    matrix must be able to say 'proven with a fake, never against the real
    thing'."""
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    b = built(
        evidence={"unit": {"TestA": pair}, "integration": {"TestB": pair}},
        results={"unit": {"TestA": PASS}, "integration": {"TestB": PASS}},
    )
    assert set(b["cells"]) == {
        ("sink.flush.keeps_batch", "sink.clickhouse", "unit"),
        ("sink.flush.keeps_batch", "sink.clickhouse", "integration"),
    }


def test_build_collects_every_test_that_proves_one_cell():
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    b = built(
        evidence={"unit": {"TestB": pair, "TestA": pair}},
        results={"unit": {"TestA": PASS, "TestB": PASS}},
    )
    key = ("sink.flush.keeps_batch", "sink.clickhouse", "unit")
    # Order is the snapshot's business, not this function's.
    assert sorted(b["cells"][key]) == [("TestA", PASS), ("TestB", PASS)]


def test_build_takes_the_outcome_from_the_result_not_the_marker():
    b = built(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": FAIL}},
    )
    key = ("sink.flush.keeps_batch", "sink.clickhouse", "unit")
    assert b["cells"][key] == [("TestA", FAIL)]


def test_build_treats_a_marker_with_no_result_as_a_failure():
    """A marker whose test has no verdict means the event stream lost it.
    Reading that as coverage would report a cell nobody proved."""
    b = built(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={},
    )
    key = ("sink.flush.keeps_batch", "sink.clickhouse", "unit")
    assert b["cells"][key] == [("TestA", FAIL)]


def test_build_reports_an_unknown_id_rather_than_inventing_a_cell():
    b = built(
        evidence={"unit": {"TestA": [("nope.nope", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert b["cells"] == {}
    assert b["unknown"] == [("TestA", "nope.nope", "sink.clickhouse")]


def test_build_over_no_evidence_is_empty_not_an_error():
    b = built()
    assert b == {"cells": {}, "unknown": []}


def test_snapshot_carries_the_declaration_onto_every_invariant():
    s = inv_snap()
    inv = next(i for i in s["invariants"] if i["id"] == "sink.flush.keeps_batch")

    assert inv["family"] == "resilience"
    assert inv["applies_to"] == "sink"
    assert inv["claim"] == "kept"
    assert inv["verified_by"] == "harness"
    assert inv["requires"] == []


def test_snapshot_folds_a_wrapped_claim_onto_one_line():
    """The YAML wraps long claims. Rewrapping one must not show up as a
    coverage change in the JSON diff a reviewer reads."""
    wrapped = [dict(INVARIANTS[0], claim="a claim\nwrapped over\nthree lines")]
    s = inv_snap(invariants=wrapped)
    assert s["invariants"][0]["claim"] == "a claim wrapped over three lines"


def test_snapshot_omits_tracked_by_and_violated_once_when_absent():
    """Same rule as the feature half: an absent fact costs no bytes."""
    s = inv_snap()
    inv = s["invariants"][0]
    assert "tracked_by" not in inv
    assert "violated_once" not in inv


def test_snapshot_carries_violated_once_when_present():
    s = inv_snap(invariants=[dict(INVARIANTS[0], violated_once=["#221"])])
    assert s["invariants"][0]["violated_once"] == ["#221"]


def test_snapshot_gives_every_level_a_cell():
    s = inv_snap()
    inv = next(i for i in s["invariants"] if i["id"] == "sink.flush.keeps_batch")
    assert set(inv["integrations"]["sink.clickhouse"]) == set(LEVELS)


def test_snapshot_names_a_skipped_test_in_the_skipped_list():
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": SKIP}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "unit")
    assert c["tests"] == ["TestA"]
    assert c["skipped"] == ["TestA"]
    assert "failing" not in c


def test_snapshot_names_a_failing_test_in_the_failing_list():
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": FAIL}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "unit")
    assert c["failing"] == ["TestA"]
    assert "skipped" not in c


def test_snapshot_omits_an_empty_exceptional_list():
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "unit") == {
        "status": "covered", "tests": ["TestA"]}


def test_snapshot_every_exceptional_name_also_appears_in_tests():
    """skipped and failing narrow tests; they never extend it. The same
    encoding rule the feature half follows."""
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    s = inv_snap(
        evidence={"unit": {"TestA": pair, "TestB": pair, "TestC": pair}},
        results={"unit": {"TestA": PASS, "TestB": SKIP, "TestC": FAIL}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "unit")
    for name in c.get("skipped", []) + c.get("failing", []):
        assert name in c["tests"]


def test_snapshot_one_pass_among_skips_covers_the_cell():
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    s = inv_snap(
        evidence={"unit": {"TestA": pair, "TestB": pair}},
        results={"unit": {"TestA": PASS, "TestB": SKIP}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "unit")["status"] == "covered"


def test_snapshot_one_failure_among_passes_fails_the_cell():
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    s = inv_snap(
        evidence={"unit": {"TestA": pair, "TestB": pair}},
        results={"unit": {"TestA": PASS, "TestB": FAIL}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "unit")["status"] == "failing"


def test_snapshot_an_exempt_cell_carries_no_tests_even_with_evidence():
    """An exemption is a statement that the invariant cannot apply. Evidence
    against it means the registry and the harness disagree, and the registry
    is the declaration -- but the cell must not read as covered."""
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.console")]}},
        results={"unit": {"TestA": PASS}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.console", "unit")
    assert c["status"] == "exempt"
    assert "tests" not in c


def test_snapshot_a_pipeline_invariant_gets_a_cell_per_configuration():
    """The consume loop has configurations, and durable state changes what a
    commit means. One cell each is what shows which configuration proved it."""
    s = inv_snap(invariants=PIPELINE)
    assert list(s["invariants"][0]["integrations"]) == ["pipeline.stateful"]


def test_snapshot_orders_tests_stably_whatever_order_they_arrived_in():
    """`go test` reports in completion order, which varies run to run. The
    same tree must still produce the same bytes, or the staleness check fails
    on noise and a reader learns to ignore it."""
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    results = {"unit": {"TestA": PASS, "TestB": PASS, "TestC": PASS}}

    forwards = inv_snap(
        evidence={"unit": {"TestA": pair, "TestB": pair, "TestC": pair}},
        results=results)
    backwards = inv_snap(
        evidence={"unit": {"TestC": pair, "TestB": pair, "TestA": pair}},
        results=results)

    assert json.dumps(forwards) == json.dumps(backwards)
    assert cell(forwards, "sink.flush.keeps_batch", "sink.clickhouse",
                "unit")["tests"] == ["TestA", "TestB", "TestC"]


def test_snapshot_orders_unknown_markers_stably():
    pair_a = ("nope.a", "sink.clickhouse")
    pair_b = ("nope.b", "sink.clickhouse")
    results = {"unit": {"TestA": PASS, "TestB": PASS}}

    forwards = inv_snap(
        evidence={"unit": {"TestA": [pair_b], "TestB": [pair_a]}}, results=results)
    backwards = inv_snap(
        evidence={"unit": {"TestB": [pair_a], "TestA": [pair_b]}}, results=results)

    assert forwards["unknown_invariant_markers"] == backwards["unknown_invariant_markers"]
    assert [m["test"] for m in forwards["unknown_invariant_markers"]] == ["TestA", "TestB"]


def test_snapshot_gap_records_the_status_that_caused_it():
    """'sink.kafka: integration is skipped' sends the reader somewhere;
    'is missing' sends them somewhere else."""
    required = [dict(INVARIANTS[0], requires=["unit"])]
    s = inv_snap(
        invariants=required,
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": SKIP}},
    )
    assert {"invariant": "sink.flush.keeps_batch", "integration": "sink.clickhouse",
            "level": "unit", "status": "skipped"} in s["invariant_gaps"]


def test_snapshot_a_covered_required_level_is_not_a_gap():
    required = [dict(INVARIANTS[0], requires=["unit"])]
    s = inv_snap(
        invariants=required,
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert not any(g["integration"] == "sink.clickhouse"
                   for g in s["invariant_gaps"])


def test_snapshot_requires_one_level_and_ignores_the_others():
    """Requiring integration must not make a missing unit cell a gap."""
    required = [dict(INVARIANTS[0], requires=["integration"])]
    s = inv_snap(invariants=required)
    levels = {g["level"] for g in s["invariant_gaps"]}
    assert levels == {"integration"}


def test_snapshot_deduplicates_an_unknown_marker_seen_twice():
    s = inv_snap(
        evidence={"unit": {"TestA": [("nope.nope", "sink.clickhouse"),
                                     ("nope.nope", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert s["unknown_invariant_markers"] == [
        {"test": "TestA", "invariant": "nope.nope", "integration": "sink.clickhouse"}]


def test_the_tally_counts_every_cell_by_state():
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}})
    tally, unwired = invariant_tally(s)

    # keeps_batch: clickhouse covered, console exempt.
    # only_processed: kafka missing.
    assert tally["covered"] == 1
    assert tally["exempt"] == 1
    assert tally["missing"] == 1
    assert unwired == 0


def test_the_tally_counts_a_pipeline_cell_like_any_other():
    """It used to carry no cell and be tallied separately as unwired."""
    tally, unwired = invariant_tally(inv_snap(invariants=PIPELINE))

    assert unwired == 0
    assert tally["missing"] == 1
    assert sum(tally.values()) == 1


def test_no_committed_invariant_is_unwired():
    """An invariant nothing can prove is a declaration with no path to
    evidence."""
    _, unwired = invariant_tally(committed_page_snapshot())
    assert unwired == 0


def test_cell_state_reports_the_worst_thing_that_happened():
    assert cell_state(levels(unit="covered", integration="failing")) == "failing"
    assert cell_state(levels(unit="covered", integration="missing")) == "covered"
    assert cell_state(levels(unit="skipped")) == "skipped"
    assert cell_state(levels()) == "missing"
    assert cell_state(
        {lvl: {"status": "exempt", "reason": "r"} for lvl in LEVELS}) == "exempt"


def test_the_snapshot_carries_the_proof_beside_the_exemption():
    """A reader looking at an exempt cell can go straight to the test."""
    integrations = [dict(INTEGRATIONS[1], exempt=[{
        "invariant": "sink.flush.keeps_batch",
        "reason": "implements no Prober",
        "proven_by": "TestSinkConsole_ImplementsNoProber"}])]
    s = inv_snap(integrations=integrations)
    c = cell(s, "sink.flush.keeps_batch", "sink.console", "unit")

    assert c["status"] == "exempt"
    assert c["proven_by"] == "TestSinkConsole_ImplementsNoProber"


def test_an_enforced_invariant_needs_evidence_at_some_level():
    enforced = [dict(INVARIANTS[0], enforced=True)]
    s = inv_snap(
        invariants=enforced,
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}},
    )
    # clickhouse is proven at unit, which is enough; console is exempt.
    assert s["invariant_gaps"] == []


def test_an_enforced_invariant_with_no_evidence_anywhere_is_a_gap():
    enforced = [dict(INVARIANTS[0], enforced=True)]
    s = inv_snap(invariants=enforced)

    assert {"invariant": "sink.flush.keeps_batch", "integration": "sink.clickhouse",
            "level": "any", "status": "missing"} in s["invariant_gaps"]
    assert not any(g["integration"] == "sink.console" for g in s["invariant_gaps"])


def test_an_enforced_invariant_proven_at_integration_only_is_covered():
    """A sink that needs a container is not penalised for needing one."""
    enforced = [dict(INVARIANTS[0], enforced=True)]
    s = inv_snap(
        invariants=enforced,
        evidence={"integration": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {"TestA": PASS}},
    )
    assert s["invariant_gaps"] == []


def test_an_enforced_invariant_whose_only_evidence_is_a_skip_is_a_gap():
    enforced = [dict(INVARIANTS[0], enforced=True)]
    s = inv_snap(
        invariants=enforced,
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": SKIP}},
    )
    assert any(g["integration"] == "sink.clickhouse" for g in s["invariant_gaps"])


def test_requires_and_enforced_are_independent():
    """requires still demands a named level when a claim genuinely needs one."""
    both = [dict(INVARIANTS[0], enforced=True, requires=["release"])]
    s = inv_snap(
        invariants=both,
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}},
    )
    levels = {g["level"] for g in s["invariant_gaps"]}
    assert levels == {"release"}


def test_a_test_only_integration_gets_no_cells():
    integrations = INTEGRATIONS + [
        {"id": "sink.conformance_double", "kind": "sink", "test_only": True,
         "implements": ["Sink"], "exempt": []}]
    s = inv_snap(integrations=integrations)
    inv = next(i for i in s["invariants"] if i["id"] == "sink.flush.keeps_batch")

    assert "sink.conformance_double" not in inv["integrations"]


def test_a_double_marker_credits_nothing():
    """The whole point: a passing double changes no cell."""
    integrations = INTEGRATIONS + [
        {"id": "sink.conformance_double", "kind": "sink", "test_only": True,
         "implements": ["Sink"], "exempt": []}]
    s = inv_snap(
        integrations=integrations,
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.conformance_double")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "unit")["status"] == "missing"
    assert s["unknown_invariant_markers"] == []


def test_a_pipeline_invariant_is_proven_by_a_marker():
    s = inv_snap(
        invariants=PIPELINE,
        evidence={"unit": {"TestA": [("pipeline.commit.after_flush",
                                      "pipeline.stateful")]}},
        results={"unit": {"TestA": PASS}},
    )
    inv = s["invariants"][0]
    assert inv["integrations"]["pipeline.stateful"]["unit"]["status"] == "covered"


def test_a_pipeline_invariant_with_no_marker_is_missing_not_absent():
    """It used to render "none collected", which reads as "not applicable"."""
    s = inv_snap(invariants=PIPELINE)
    inv = s["invariants"][0]
    assert inv["integrations"]["pipeline.stateful"]["unit"]["status"] == "missing"


def test_a_pipeline_marker_naming_a_sink_is_unknown():
    """applies_to is the guard. A pipeline invariant proven "on sink.kafka"
    is a typo, and crediting it would invent a cell."""
    s = inv_snap(
        invariants=PIPELINE,
        evidence={"unit": {"TestA": [("pipeline.commit.after_flush", "sink.kafka")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert {"test": "TestA", "invariant": "pipeline.commit.after_flush",
            "integration": "sink.kafka"} in s["unknown_invariant_markers"]


def test_a_sink_marker_naming_the_pipeline_is_unknown():
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "pipeline")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert {"test": "TestA", "invariant": "sink.flush.keeps_batch",
            "integration": "pipeline"} in s["unknown_invariant_markers"]


def test_a_sink_marker_naming_a_source_is_unknown():
    """The same guard catches a sink invariant credited to a source."""
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "source.kafka")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert {"test": "TestA", "invariant": "sink.flush.keeps_batch",
            "integration": "source.kafka"} in s["unknown_invariant_markers"]


def test_the_page_snapshot_takes_exempt_from_the_registry():
    """The registry is the declaration. A status file that disagrees is
    stale, and the gate says so; the page does not repeat the disagreement."""
    ps = page_of(status={"features": {}, "integrations": {
        "sink.console": {"sink.flush.keeps_batch": {
            "unit": "covered", "integration": "covered", "release": "covered"}}}})
    assert cell_state(
        ps["invariants"][0]["integrations"]["sink.console"]) == "exempt"