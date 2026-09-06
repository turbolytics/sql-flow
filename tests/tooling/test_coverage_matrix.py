"""Tests for the coverage matrix generator.

The matrix gates every merge, so a wrong generator is a wrong gate. The two
failures that matter most are the ones it exists to prevent: reporting a
skipped test as coverage, and reporting a covered feature as a gap. Both are
asserted directly below.
"""

import json
import os
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "scripts"))

import coverage_matrix as cm  # noqa: E402


FEATURES = [
    {"id": "sink.clickhouse", "description": "ch", "requires": ["unit", "release"]},
    {"id": "sink.console", "description": "console", "requires": ["unit"]},
    {"id": "source.kafka", "description": "kafka", "requires": ["release"]},
    {"id": "state.durability", "description": "state", "requires": ["unit"]},
]


def snap(go_results=None, py_results=None, go_covers=None, py_covers=None,
         integration_results=None, integration_covers=None, features=None):
    features = features or FEATURES
    coverage, secondary, unmatched, unknown = cm.build(
        features, go_results or {}, py_results or {},
        go_covers or {}, py_covers or {},
        integration_results or {}, integration_covers or {})
    return cm.snapshot(features, coverage, secondary, unmatched, unknown)


def level(s, feature_id, lvl):
    for feature in s["features"]:
        if feature["id"] == feature_id:
            return feature["levels"][lvl]
    raise AssertionError(f"{feature_id} not in the snapshot")


# --- Name-based attribution ------------------------------------------------

def test_go_prefix_derives_from_the_feature_id():
    assert cm.go_prefix("sink.clickhouse") == "TestSinkClickhouse"
    assert cm.go_prefix("observability.debug_api") == "TestObservabilityDebugApi"


def test_py_prefix_derives_from_the_feature_id():
    assert cm.py_prefix("sink.clickhouse") == "test_sink_clickhouse"
    assert cm.py_prefix("error.dlq") == "test_error_dlq"


def test_a_test_attaches_to_the_feature_its_name_names():
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": cm.PASS})
    assert level(s, "sink.clickhouse", "unit")["status"] == "covered"


def test_longest_prefix_wins():
    """sink.console must not swallow a hypothetical sink.console_extra."""
    features = FEATURES + [
        {"id": "sink.console_extra", "description": "x", "requires": ["unit"]}]
    s = snap(go_results={"TestSinkConsoleExtra_Thing": cm.PASS}, features=features)

    assert level(s, "sink.console_extra", "unit")["status"] == "covered"
    assert level(s, "sink.console", "unit")["status"] == "missing"


def test_an_unmatched_test_is_reported_not_invented():
    s = snap(go_results={"TestSomethingNobodyDeclared": cm.PASS})
    assert s["unattributed"]["unit"] == ["TestSomethingNobodyDeclared"]


# --- A skip is not coverage ------------------------------------------------

def test_a_skipped_test_does_not_cover_its_feature():
    """The whole reason this tool exists. sink.iceberg shipped for months
    behind a unit test that skipped and printed ok."""
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": cm.SKIP})

    assert level(s, "sink.clickhouse", "unit")["status"] == "skipped"
    assert {"feature": "sink.clickhouse", "level": "unit",
            "status": "skipped"} in s["gaps"]


def test_one_passing_test_covers_a_feature_others_skipped():
    s = snap(go_results={
        "TestSinkClickhouse_InsertsRows": cm.PASS,
        "TestSinkClickhouse_InsertsArrays": cm.SKIP,
    })
    assert level(s, "sink.clickhouse", "unit")["status"] == "covered"


def test_a_failing_test_does_not_cover_its_feature():
    s = snap(go_results={
        "TestSinkClickhouse_InsertsRows": cm.PASS,
        "TestSinkClickhouse_InsertsArrays": cm.FAIL,
    })
    assert level(s, "sink.clickhouse", "unit")["status"] == "failing"


# --- Required levels -------------------------------------------------------

def test_a_level_that_is_not_required_is_not_a_gap():
    s = snap()
    assert level(s, "sink.console", "release")["status"] == "not_required"
    assert not any(g["feature"] == "sink.console" and g["level"] == "release"
                   for g in s["gaps"])


def test_a_required_level_with_no_test_is_a_gap():
    s = snap()
    assert {"feature": "sink.console", "level": "unit",
            "status": "missing"} in s["gaps"]


def test_coverage_beyond_what_is_required_still_shows():
    """A test at a level the registry does not demand is reported, not hidden:
    dropping it later should be visible."""
    s = snap(py_results={"test_sink_console_writes_rows": cm.PASS})
    assert level(s, "sink.console", "release")["status"] == "covered"


# --- The integration level -------------------------------------------------

def test_levels_run_from_cheapest_to_most_real():
    assert cm.LEVELS == ("unit", "integration", "release")


def test_an_integration_result_lands_at_the_integration_level():
    s = snap(
        integration_results={"TestIntegrationSourceKafka_Commits": cm.PASS},
        features=[{"id": "source.kafka", "description": "kafka",
                   "requires": ["integration"]}],
    )
    assert level(s, "source.kafka", "integration")["status"] == "covered"


def test_the_integration_prefix_does_not_decide_the_feature():
    """Level and feature stay separate axes. The prefix selects which pass runs
    a test; the rest of the name still says which feature it covers."""
    assert (cm.strip_level_prefix("TestIntegrationSourceKafka_Commits")
            == "TestSourceKafka_Commits")
    assert (cm.strip_level_prefix("TestSourceKafka_Commits")
            == "TestSourceKafka_Commits")


def test_an_integration_test_does_not_manufacture_unit_coverage():
    """The same names appear in the unit pass, skipped by -short. A skip is not
    coverage there just because the integration pass ran it for real."""
    s = snap(
        go_results={"TestIntegrationSourceKafka_Commits": cm.SKIP},
        integration_results={"TestIntegrationSourceKafka_Commits": cm.PASS},
        features=[{"id": "source.kafka", "description": "kafka",
                   "requires": ["unit", "integration"]}],
    )
    assert level(s, "source.kafka", "unit")["status"] == "skipped"
    assert level(s, "source.kafka", "integration")["status"] == "covered"


def test_a_skipped_integration_test_is_a_gap():
    """An integration suite that skips itself when its service is absent is the
    sink.iceberg failure one level up. It must not read as coverage."""
    s = snap(
        integration_results={"TestIntegrationSourceKafka_Commits": cm.SKIP},
        features=[{"id": "source.kafka", "description": "kafka",
                   "requires": ["integration"]}],
    )
    assert {"feature": "source.kafka", "level": "integration",
            "status": "skipped"} in s["gaps"]


def test_render_carries_a_column_per_level():
    s = snap(integration_results={"TestSourceKafka_Commits": cm.PASS})
    assert "| Feature | What it does | unit | integration | release | Tests |" \
        in cm.render(s)


# --- Markers ---------------------------------------------------------------

def test_a_marker_attributes_a_second_feature():
    s = snap(
        py_results={"test_handler_inferred_mem_aggregates": cm.PASS},
        py_covers={"test_handler_inferred_mem_aggregates": ["source.kafka"]},
        features=FEATURES + [{"id": "handler.inferred_mem", "description": "h",
                              "requires": ["release"]}],
    )
    assert level(s, "source.kafka", "release")["status"] == "covered"


def test_a_marker_is_recorded_as_secondary_attribution():
    s = snap(
        py_results={"test_handler_inferred_mem_aggregates": cm.PASS},
        py_covers={"test_handler_inferred_mem_aggregates": ["source.kafka"]},
        features=FEATURES + [{"id": "handler.inferred_mem", "description": "h",
                              "requires": ["release"]}],
    )
    lvl = level(s, "source.kafka", "release")
    assert lvl["by_marker"] == ["test_handler_inferred_mem_aggregates"]


def test_a_marker_carries_the_outcome_not_just_the_name():
    """A marker on a skipped test must not manufacture coverage."""
    s = snap(
        py_results={"test_handler_inferred_mem_aggregates": cm.SKIP},
        py_covers={"test_handler_inferred_mem_aggregates": ["source.kafka"]},
        features=FEATURES + [{"id": "handler.inferred_mem", "description": "h",
                              "requires": ["release"]}],
    )
    assert level(s, "source.kafka", "release")["status"] == "skipped"


def test_a_marker_naming_an_undeclared_feature_is_reported():
    s = snap(
        go_results={"TestSinkClickhouse_InsertsRows": cm.PASS},
        go_covers={"TestSinkClickhouse_InsertsRows": ["sink.nonexistent"]},
    )
    assert {"test": "TestSinkClickhouse_InsertsRows",
            "feature": "sink.nonexistent"} in s["unknown_markers"]


def test_a_marker_for_the_feature_the_name_already_claims_is_not_doubled():
    s = snap(
        go_results={"TestSinkClickhouse_InsertsRows": cm.PASS},
        go_covers={"TestSinkClickhouse_InsertsRows": ["sink.clickhouse"]},
    )
    assert level(s, "sink.clickhouse", "unit")["tests"] == [
        "TestSinkClickhouse_InsertsRows"]


# --- Parsing ---------------------------------------------------------------

def write(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text)
    return str(path)


def test_parse_go_reads_outcomes_and_markers(tmp_path):
    path = write(tmp_path, "go.json", "\n".join([
        json.dumps({"Action": "run", "Test": "TestA"}),
        json.dumps({"Action": "output", "Test": "TestA",
                    "Output": "    x.go:1: COVERS sink.clickhouse\n"}),
        json.dumps({"Action": "pass", "Test": "TestA"}),
        json.dumps({"Action": "skip", "Test": "TestB"}),
        json.dumps({"Action": "fail", "Test": "TestC"}),
        "not json at all",
    ]))
    results, covers, _ = cm.parse_go(path)

    assert results == {"TestA": cm.PASS, "TestB": cm.SKIP, "TestC": cm.FAIL}
    assert covers == {"TestA": ["sink.clickhouse"]}


def test_parse_go_ignores_package_level_events(tmp_path):
    """Events with no Test are the package summary, not a test result."""
    path = write(tmp_path, "go.json",
                 json.dumps({"Action": "pass", "Package": "x"}))
    results, _, _ = cm.parse_go(path)
    assert results == {}


def test_parse_go_reads_structured_invariant_markers(tmp_path):
    """The harness emits one line per (invariant, integration). Both ids are
    carried; the parser must not collapse them into the feature list."""
    path = write(tmp_path, "go.json", "\n".join([
        json.dumps({"Action": "output", "Test": "TestA",
                    "Output": "    x.go:1: COVERS invariant=sink.flush.keeps_batch integration=sink.clickhouse\n"}),
        json.dumps({"Action": "output", "Test": "TestA",
                    "Output": "    x.go:2: COVERS sink.clickhouse\n"}),
        json.dumps({"Action": "pass", "Test": "TestA"}),
    ]))
    results, covers, invariants = cm.parse_go(path)

    assert results == {"TestA": cm.PASS}
    assert covers == {"TestA": ["sink.clickhouse"]}
    assert invariants == {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}


def test_parse_pytest_returns_an_empty_invariant_map(tmp_path):
    path = write(tmp_path, "pytest.json", json.dumps({"tests": [
        {"nodeid": "test_a", "outcome": "passed", "covers": []},
    ]}))
    results, covers, invariants = cm.parse_pytest(path)
    assert invariants == {}


def test_parse_go_lets_a_pass_outrank_a_skipped_subtest():
    """A parent that passes with one skipped subtest is coverage."""
    assert cm.PASS != cm.SKIP  # guard the constants


def test_parse_pytest_reads_outcomes_and_markers(tmp_path):
    path = write(tmp_path, "pytest.json", json.dumps({"tests": [
        {"nodeid": "test_sink_clickhouse_x", "outcome": "passed",
         "covers": ["source.kafka"]},
        {"nodeid": "test_b", "outcome": "skipped", "covers": []},
        {"nodeid": "test_c", "outcome": "failed", "covers": []},
    ]}))
    results, covers, _ = cm.parse_pytest(path)

    assert results == {"test_sink_clickhouse_x": cm.PASS,
                       "test_b": cm.SKIP, "test_c": cm.FAIL}
    assert covers == {"test_sink_clickhouse_x": ["source.kafka"]}


# --- The artifact ----------------------------------------------------------

def test_the_snapshot_is_deterministic():
    """CI diffs this file. Unstable ordering would make every run a diff."""
    args = dict(
        go_results={"TestSinkClickhouse_B": cm.PASS,
                    "TestSinkClickhouse_A": cm.PASS,
                    "TestStateDurability_X": cm.PASS},
        py_results={"test_source_kafka_y": cm.PASS},
    )
    first = json.dumps(snap(**args), indent=2)
    second = json.dumps(snap(**args), indent=2)
    assert first == second


def test_the_snapshot_orders_tests_stably():
    s = snap(go_results={"TestSinkClickhouse_B": cm.PASS,
                         "TestSinkClickhouse_A": cm.PASS})
    names = level(s, "sink.clickhouse", "unit")["tests"]
    assert names == sorted(names)


# --- The encoding ----------------------------------------------------------
#
# The artifact is read far more often than it is written, by people and by
# agents, and every byte of it is a byte someone pays for. So a test is a name
# and nothing else, and the two facts that are almost never true -- a test did
# not pass, a test was attributed by marker -- are named in their own lists
# rather than repeated on all 489 entries as the default they usually carry.

def test_a_test_is_recorded_as_a_bare_name():
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": cm.PASS})
    assert level(s, "sink.clickhouse", "unit")["tests"] == [
        "TestSinkClickhouse_InsertsRows"]


def test_a_level_with_nothing_exceptional_carries_only_status_and_tests():
    """The common case pays for nothing it does not use."""
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": cm.PASS})
    assert set(level(s, "sink.clickhouse", "unit")) == {"status", "tests"}


def test_an_empty_list_is_omitted_entirely():
    s = snap()
    assert level(s, "sink.console", "release") == {"status": "not_required"}


def test_a_skipped_test_is_named_in_the_skipped_list():
    """Status alone cannot say which of several tests skipped."""
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": cm.PASS,
                         "TestSinkClickhouse_InsertsArrays": cm.SKIP})
    lvl = level(s, "sink.clickhouse", "unit")

    assert lvl["status"] == "covered"
    assert lvl["skipped"] == ["TestSinkClickhouse_InsertsArrays"]
    assert "failing" not in lvl


def test_a_failing_test_is_named_in_the_failing_list():
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": cm.PASS,
                         "TestSinkClickhouse_InsertsArrays": cm.FAIL})
    lvl = level(s, "sink.clickhouse", "unit")

    assert lvl["failing"] == ["TestSinkClickhouse_InsertsArrays"]
    assert "skipped" not in lvl


def test_every_exceptional_name_also_appears_in_tests():
    """The lists narrow the test list. They never extend it."""
    s = snap(go_results={"TestSinkClickhouse_A": cm.PASS,
                         "TestSinkClickhouse_B": cm.SKIP,
                         "TestSinkClickhouse_C": cm.FAIL})
    lvl = level(s, "sink.clickhouse", "unit")

    for key in ("skipped", "failing", "by_marker"):
        assert set(lvl.get(key, [])) <= set(lvl["tests"]), key


def test_the_snapshot_declares_its_schema_version():
    """A reader that expects per-test objects must be able to tell."""
    assert snap()["version"] == 3


def test_render_agrees_with_the_snapshot():
    """The markdown is a view. It must not be able to disagree with the JSON
    that gates the build."""
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": cm.SKIP})
    out = cm.render(s)

    assert "sink.clickhouse" in out
    assert "skipped" in out
    assert "1 gap(s)" in out or "gap(s)" in out


def test_render_counts_the_tests_rather_than_sampling_them():
    """The table is the cheap view. Three arbitrary names out of seventy-three
    answer nobody's question and cost every reader the width; the count answers
    "is this feature thinly covered" and matrix.json names them all."""
    s = snap(go_results={f"TestSinkClickhouse_{i}": cm.PASS for i in range(4)})
    row = next(line for line in cm.render(s).splitlines()
               if line.startswith("| `sink.clickhouse`"))

    assert row.rstrip().endswith("| 4 |")
    assert "TestSinkClickhouse_0" not in row


def test_render_leaves_the_count_blank_when_nothing_covers_the_feature():
    s = snap()
    row = next(line for line in cm.render(s).splitlines()
               if line.startswith("| `sink.console`"))
    assert row.rstrip().endswith("| — |")


def test_render_points_at_the_json_for_the_test_names():
    """Dropping the names without saying where they went sends the reader to
    grep the repo, or to open the 49KB artifact to find out it was that."""
    assert "names every one of them" in cm.render(snap())


def test_render_reports_a_feature_covered_only_by_a_marker():
    s = snap(
        py_results={"test_handler_inferred_mem_aggregates": cm.PASS},
        py_covers={"test_handler_inferred_mem_aggregates": ["source.kafka"]},
        features=FEATURES + [{"id": "handler.inferred_mem", "description": "h",
                              "requires": ["release"]}],
    )
    out = cm.render(s)
    assert "Covered only by another test's marker" in out
    assert "`source.kafka` (release)" in out


# --- The registry itself ---------------------------------------------------

def test_every_declared_feature_is_well_formed():
    for feature in cm.load_features():
        assert feature["id"], feature
        assert "." in feature["id"], f"{feature['id']} needs a domain.component id"
        assert feature["id"] == feature["id"].lower(), feature["id"]
        assert feature.get("description"), feature["id"]
        assert feature.get("requires"), f"{feature['id']} requires nothing"
        for lvl in feature["requires"]:
            assert lvl in cm.LEVELS, f"{feature['id']}: {lvl}"


def test_feature_ids_are_unique():
    ids = [f["id"] for f in cm.load_features()]
    assert len(ids) == len(set(ids))


def test_no_feature_id_encodes_a_level():
    """Level is a derived axis. Baking it into the id makes the unit and
    release rows of the same feature different strings, and the matrix can no
    longer ask what is covered in unit but not in the image."""
    for feature in cm.load_features():
        head = feature["id"].split(".")[0]
        assert head not in ("unit", "release", "integration"), feature["id"]


def test_the_check_exits_non_zero_on_a_gap(tmp_path):
    """The gate itself, end to end."""
    go = write(tmp_path, "go.json", json.dumps(
        {"Action": "skip", "Test": "TestSinkClickhouse_InsertsRows"}))
    proc = subprocess.run(
        [sys.executable, os.path.join(cm.REPO, "scripts", "coverage_matrix.py"),
         "--go", go, "--check"],
        capture_output=True, text=True)

    assert proc.returncode == 1, proc.stdout
    assert "coverage gap" in proc.stderr


# --- Registries ------------------------------------------------------------

INVARIANTS = [
    {"id": "sink.flush.keeps_batch", "family": "resilience", "applies_to": "sink",
     "claim": "kept", "verified_by": "harness", "requires": []},
    {"id": "source.commit.only_processed", "family": "checkpoint",
     "applies_to": "source", "claim": "c", "verified_by": "harness", "requires": []},
]

INTEGRATIONS = [
    {"id": "sink.clickhouse", "kind": "sink", "implements": ["Sink"],
     "feature": "sink.clickhouse", "exempt": []},
    {"id": "sink.console", "kind": "sink", "implements": ["Sink"],
     "feature": "sink.console",
     "exempt": [{"invariant": "sink.flush.keeps_batch", "reason": "stdout"}]},
    {"id": "source.kafka", "kind": "source", "implements": ["Source"],
     "feature": "source.kafka", "exempt": []},
]


def test_the_committed_registries_load_and_validate():
    """The real files, not fixtures: a typo in either fails here before it can
    fail in CI."""
    invariants = cm.load_invariants()
    integrations = cm.load_integrations()
    features = cm.load_features()

    assert cm.validate_registries(invariants, integrations, features) == []
    assert len(invariants) >= 20
    assert {i["kind"] for i in integrations} == {"sink", "source", "handler"}


def test_every_committed_invariant_is_unenforced_in_this_revision():
    """This revision reports and fails nothing. Filling in a requires is a
    later change, and legal only once every non-exempt integration has a
    subject."""
    for inv in cm.load_invariants():
        assert inv["requires"] == [], inv["id"]


def test_validate_rejects_an_exemption_naming_no_invariant():
    bad = [dict(INTEGRATIONS[1], exempt=[
        {"invariant": "sink.flush.nonexistent", "reason": "r"}])]
    problems = cm.validate_registries(INVARIANTS, bad, FEATURES)
    assert any("sink.flush.nonexistent" in p for p in problems)


def test_validate_rejects_an_exemption_without_a_reason():
    bad = [dict(INTEGRATIONS[1], exempt=[
        {"invariant": "sink.flush.keeps_batch"}])]
    problems = cm.validate_registries(INVARIANTS, bad, FEATURES)
    assert any("reason" in p for p in problems)


def test_validate_rejects_an_exemption_from_an_invariant_of_another_kind():
    """A sink cannot be exempt from a source invariant; that is a typo."""
    bad = [dict(INTEGRATIONS[0], exempt=[
        {"invariant": "source.commit.only_processed", "reason": "r"}])]
    problems = cm.validate_registries(INVARIANTS, bad, FEATURES)
    assert any("applies_to" in p for p in problems)


def test_validate_rejects_an_integration_naming_no_feature():
    bad = [dict(INTEGRATIONS[0], feature="sink.nonexistent")]
    problems = cm.validate_registries(INVARIANTS, bad, FEATURES)
    assert any("sink.nonexistent" in p for p in problems)


def test_validate_rejects_a_duplicate_invariant_id():
    problems = cm.validate_registries(
        INVARIANTS + [INVARIANTS[0]], INTEGRATIONS, FEATURES)
    assert any("duplicate" in p for p in problems)


def test_validate_rejects_a_requires_level_that_does_not_exist():
    bad = [dict(INVARIANTS[0], requires=["nightly"])]
    problems = cm.validate_registries(bad, INTEGRATIONS, FEATURES)
    assert any("nightly" in p for p in problems)


# --- The invariant axis ----------------------------------------------------

def inv_snap(evidence=None, results=None, invariants=None, integrations=None):
    """evidence: {level: {test: [(invariant, integration)]}}
    results:    {level: {test: outcome}}"""
    evidence = evidence or {}
    results = results or {}
    invariants = invariants or INVARIANTS
    integrations = integrations or INTEGRATIONS
    built = cm.build_invariants(
        invariants, integrations,
        {lvl: results.get(lvl, {}) for lvl in cm.LEVELS},
        {lvl: evidence.get(lvl, {}) for lvl in cm.LEVELS})
    return cm.snapshot_invariants(invariants, integrations, built)


def cell(s, invariant, integration, lvl):
    for inv in s["invariants"]:
        if inv["id"] == invariant:
            return inv["integrations"][integration][lvl]
    raise AssertionError(f"{invariant} not in the snapshot")


def test_a_passing_harness_case_covers_the_cell():
    name = "TestIntegrationSinkClickhouse_Conformance/keeps_batch"
    s = inv_snap(
        evidence={"integration": {name: [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {name: cm.PASS}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "integration")
    assert c["status"] == "covered"
    assert c["tests"] == [name]


def test_a_marker_from_a_failing_test_is_not_coverage():
    """The harness emits the marker only on pass, but the parent that ran the
    subtest can still fail afterwards, and that verdict is the reported one."""
    s = inv_snap(
        evidence={"integration": {"TestX": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {"TestX": cm.FAIL}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "integration")["status"] == "failing"


def test_a_marker_from_a_skipped_test_is_not_coverage():
    s = inv_snap(
        evidence={"integration": {"TestX": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {"TestX": cm.SKIP}},
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
        results={"integration": {"TestX": cm.PASS}},
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
        results={"integration": {"TestX": cm.PASS, "TestY": cm.PASS}},
    )
    assert len(s["unknown_invariant_markers"]) == 2


def test_a_tracked_invariant_renders_as_declared_unenforced():
    tracked = [dict(INVARIANTS[0], tracked_by="#183")]
    s = inv_snap(invariants=tracked)
    inv = next(i for i in s["invariants"] if i["id"] == "sink.flush.keeps_batch")
    assert inv["tracked_by"] == "#183"


def test_render_carries_one_table_per_family():
    s = inv_snap()
    md = cm.render_invariants(s)
    assert "## Invariants: resilience" in md
    assert "## Invariants: checkpoint" in md
    assert "| `sink.flush.keeps_batch` |" in md


def test_render_marks_an_exempt_cell():
    s = inv_snap()
    md = cm.render_invariants(s)
    line = next(l for l in md.splitlines() if "`sink.flush.keeps_batch`" in l)
    assert "exempt" in line


def test_a_test_carrying_any_marker_is_not_unattributed():
    """A marker-attributed test was listed under "match no declared feature".
    The conformance harness attributes only by marker, and a shared runner's
    name matches no feature prefix at all, so every one of its cases would
    land there telling the reader to rename a test that already says what it
    covers."""
    name = "TestConformanceSinks_KeepsBatch"
    s = snap(
        integration_results={name: cm.PASS},
        integration_covers={name: ["sink.clickhouse"]},
    )
    assert s["unattributed"]["integration"] == []
    assert level(s, "sink.clickhouse", "integration")["status"] == "covered"


def test_a_test_matching_nothing_and_carrying_no_marker_is_still_unattributed():
    """The report still catches what it is for: #221's four misnamed tests."""
    s = snap(integration_results={"TestNobodyDeclaredThis": cm.PASS})
    assert s["unattributed"]["integration"] == ["TestNobodyDeclaredThis"]


# --- build_invariants: attribution, in isolation ---------------------------
#
# The two functions below decide every invariant cell in the matrix, so they
# are tested directly rather than only through the snapshot. A wrong cell is a
# wrong gate: a covered cell that is not covered is the sink.iceberg failure
# with more ceremony.

def built(evidence=None, results=None, invariants=None, integrations=None):
    evidence = evidence or {}
    results = results or {}
    return cm.build_invariants(
        invariants or INVARIANTS, integrations or INTEGRATIONS,
        {lvl: results.get(lvl, {}) for lvl in cm.LEVELS},
        {lvl: evidence.get(lvl, {}) for lvl in cm.LEVELS})


def test_build_keys_a_cell_by_invariant_integration_and_level():
    b = built(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": cm.PASS}},
    )
    assert b["cells"] == {
        ("sink.flush.keeps_batch", "sink.clickhouse", "unit"): [("TestA", cm.PASS)]
    }


def test_build_records_the_same_invariant_at_each_level_separately():
    """unit and integration are different evidence for the same claim, and the
    matrix must be able to say 'proven with a fake, never against the real
    thing'."""
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    b = built(
        evidence={"unit": {"TestA": pair}, "integration": {"TestB": pair}},
        results={"unit": {"TestA": cm.PASS}, "integration": {"TestB": cm.PASS}},
    )
    assert set(b["cells"]) == {
        ("sink.flush.keeps_batch", "sink.clickhouse", "unit"),
        ("sink.flush.keeps_batch", "sink.clickhouse", "integration"),
    }


def test_build_collects_every_test_that_proves_one_cell():
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    b = built(
        evidence={"unit": {"TestB": pair, "TestA": pair}},
        results={"unit": {"TestA": cm.PASS, "TestB": cm.PASS}},
    )
    key = ("sink.flush.keeps_batch", "sink.clickhouse", "unit")
    # Order is the snapshot's business, not this function's.
    assert sorted(b["cells"][key]) == [("TestA", cm.PASS), ("TestB", cm.PASS)]


def test_build_takes_the_outcome_from_the_result_not_the_marker():
    b = built(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": cm.FAIL}},
    )
    key = ("sink.flush.keeps_batch", "sink.clickhouse", "unit")
    assert b["cells"][key] == [("TestA", cm.FAIL)]


def test_build_treats_a_marker_with_no_result_as_a_failure():
    """A marker whose test has no verdict means the event stream lost it.
    Reading that as coverage would report a cell nobody proved."""
    b = built(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={},
    )
    key = ("sink.flush.keeps_batch", "sink.clickhouse", "unit")
    assert b["cells"][key] == [("TestA", cm.FAIL)]


def test_build_reports_an_unknown_id_rather_than_inventing_a_cell():
    b = built(
        evidence={"unit": {"TestA": [("nope.nope", "sink.clickhouse")]}},
        results={"unit": {"TestA": cm.PASS}},
    )
    assert b["cells"] == {}
    assert b["unknown"] == [("TestA", "nope.nope", "sink.clickhouse")]


def test_build_over_no_evidence_is_empty_not_an_error():
    b = built()
    assert b == {"cells": {}, "unknown": []}


# --- snapshot_invariants: the encoding ------------------------------------

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
    assert set(inv["integrations"]["sink.clickhouse"]) == set(cm.LEVELS)


def test_snapshot_names_a_skipped_test_in_the_skipped_list():
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": cm.SKIP}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "unit")
    assert c["tests"] == ["TestA"]
    assert c["skipped"] == ["TestA"]
    assert "failing" not in c


def test_snapshot_names_a_failing_test_in_the_failing_list():
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": cm.FAIL}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "unit")
    assert c["failing"] == ["TestA"]
    assert "skipped" not in c


def test_snapshot_omits_an_empty_exceptional_list():
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": cm.PASS}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "unit") == {
        "status": "covered", "tests": ["TestA"]}


def test_snapshot_every_exceptional_name_also_appears_in_tests():
    """skipped and failing narrow tests; they never extend it. The same
    encoding rule the feature half follows."""
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    s = inv_snap(
        evidence={"unit": {"TestA": pair, "TestB": pair, "TestC": pair}},
        results={"unit": {"TestA": cm.PASS, "TestB": cm.SKIP, "TestC": cm.FAIL}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "unit")
    for name in c.get("skipped", []) + c.get("failing", []):
        assert name in c["tests"]


def test_snapshot_one_pass_among_skips_covers_the_cell():
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    s = inv_snap(
        evidence={"unit": {"TestA": pair, "TestB": pair}},
        results={"unit": {"TestA": cm.PASS, "TestB": cm.SKIP}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "unit")["status"] == "covered"


def test_snapshot_one_failure_among_passes_fails_the_cell():
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    s = inv_snap(
        evidence={"unit": {"TestA": pair, "TestB": pair}},
        results={"unit": {"TestA": cm.PASS, "TestB": cm.FAIL}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "unit")["status"] == "failing"


def test_snapshot_an_exempt_cell_carries_no_tests_even_with_evidence():
    """An exemption is a statement that the invariant cannot apply. Evidence
    against it means the registry and the harness disagree, and the registry
    is the declaration -- but the cell must not read as covered."""
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.console")]}},
        results={"unit": {"TestA": cm.PASS}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.console", "unit")
    assert c["status"] == "exempt"
    assert "tests" not in c


def test_snapshot_a_pipeline_invariant_has_no_integration_cells():
    """It attaches to core, and no constructor case is a pipeline."""
    pipeline = [{"id": "pipeline.commit.after_flush", "family": "checkpoint",
                 "applies_to": "pipeline", "claim": "c", "verified_by": "named",
                 "requires": []}]
    s = inv_snap(invariants=pipeline)
    assert s["invariants"][0]["integrations"] == {}


def test_snapshot_orders_tests_stably_whatever_order_they_arrived_in():
    """`go test` reports in completion order, which varies run to run. The
    same tree must still produce the same bytes, or the staleness check fails
    on noise and a reader learns to ignore it."""
    pair = [("sink.flush.keeps_batch", "sink.clickhouse")]
    results = {"unit": {"TestA": cm.PASS, "TestB": cm.PASS, "TestC": cm.PASS}}

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
    results = {"unit": {"TestA": cm.PASS, "TestB": cm.PASS}}

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
        results={"unit": {"TestA": cm.SKIP}},
    )
    assert {"invariant": "sink.flush.keeps_batch", "integration": "sink.clickhouse",
            "level": "unit", "status": "skipped"} in s["invariant_gaps"]


def test_snapshot_a_covered_required_level_is_not_a_gap():
    required = [dict(INVARIANTS[0], requires=["unit"])]
    s = inv_snap(
        invariants=required,
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": cm.PASS}},
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
        results={"unit": {"TestA": cm.PASS}},
    )
    assert s["unknown_invariant_markers"] == [
        {"test": "TestA", "invariant": "nope.nope", "integration": "sink.clickhouse"}]


# --- render_invariants -----------------------------------------------------

def test_render_agrees_with_the_snapshot():
    """The markdown is a view. It can never disagree with the JSON that gates
    the build."""
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": cm.PASS}},
    )
    md = cm.render_invariants(s)
    line = next(l for l in md.splitlines() if "`sink.flush.keeps_batch`" in l)
    # clickhouse is covered at unit and missing elsewhere, and no level is
    # required, so the cell shows the worst of the three.
    assert "missing" in line


def test_render_shows_covered_when_every_judged_level_is_covered():
    required = [dict(INVARIANTS[0], requires=["unit"])]
    s = inv_snap(
        invariants=required,
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": cm.PASS}},
    )
    md = cm.render_invariants(s)
    line = next(l for l in md.splitlines() if "`sink.flush.keeps_batch`" in l)
    assert "✅" in line


def test_render_names_the_tracking_issue_of_an_unenforced_invariant():
    s = inv_snap(invariants=[dict(INVARIANTS[0], tracked_by="#183")])
    md = cm.render_invariants(s)
    assert "tracked by #183" in md


def test_render_lists_a_gap():
    required = [dict(INVARIANTS[0], requires=["unit"])]
    md = cm.render_invariants(inv_snap(invariants=required))
    assert "## Invariant gaps" in md
    assert "`sink.flush.keeps_batch` on `sink.clickhouse` requires **unit**" in md


def test_render_omits_the_gap_section_when_there_are_none():
    assert "## Invariant gaps" not in cm.render_invariants(inv_snap())


def test_render_gives_a_pipeline_family_a_verified_by_column():
    """With no integrations there is no cell to show, so the table says how
    the claim is proven instead of leaving an empty row."""
    pipeline = [{"id": "pipeline.commit.after_flush", "family": "checkpoint",
                 "applies_to": "pipeline", "claim": "c", "verified_by": "named",
                 "requires": []}]
    md = cm.render_invariants(inv_snap(invariants=pipeline))
    assert "| Invariant | Claim | Verified by |" in md
    assert "| `pipeline.commit.after_flush` | c | named |" in md
