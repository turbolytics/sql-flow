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
         features=None):
    features = features or FEATURES
    coverage, secondary, unmatched, unknown = cm.build(
        features, go_results or {}, py_results or {},
        go_covers or {}, py_covers or {})
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
    tests = level(s, "source.kafka", "release")["tests"]
    assert [t["attribution"] for t in tests] == ["marker"]


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
    assert len(level(s, "sink.clickhouse", "unit")["tests"]) == 1


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
    results, covers = cm.parse_go(path)

    assert results == {"TestA": cm.PASS, "TestB": cm.SKIP, "TestC": cm.FAIL}
    assert covers == {"TestA": ["sink.clickhouse"]}


def test_parse_go_ignores_package_level_events(tmp_path):
    """Events with no Test are the package summary, not a test result."""
    path = write(tmp_path, "go.json",
                 json.dumps({"Action": "pass", "Package": "x"}))
    results, _ = cm.parse_go(path)
    assert results == {}


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
    results, covers = cm.parse_pytest(path)

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
    names = [t["name"] for t in level(s, "sink.clickhouse", "unit")["tests"]]
    assert names == sorted(names)


def test_render_agrees_with_the_snapshot():
    """The markdown is a view. It must not be able to disagree with the JSON
    that gates the build."""
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": cm.SKIP})
    out = cm.render(s)

    assert "sink.clickhouse" in out
    assert "skipped" in out
    assert "1 gap(s)" in out or "gap(s)" in out


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
            assert lvl in ("unit", "release"), f"{feature['id']}: {lvl}"


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
