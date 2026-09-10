"""Tests for coverage_matrix.features."""

import json

from coverage_matrix.features import build, feature_gaps, snapshot
from coverage_matrix.registries import FAIL, PASS, SKIP
from samples import *  # noqa: F401,F403


def test_a_test_attaches_to_the_feature_its_name_names():
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": PASS},
             go_covers={"TestSinkClickhouse_InsertsRows": ["sink.clickhouse"]})
    assert level(s, "sink.clickhouse", "unit")["status"] == "covered"


def test_an_unmatched_test_is_reported_not_invented():
    s = snap(go_results={"TestSomethingNobodyDeclared": PASS})
    assert s["unattributed"]["unit"] == ["TestSomethingNobodyDeclared"]


def test_a_skipped_test_does_not_cover_its_feature():
    """The whole reason this tool exists. sink.iceberg shipped for months
    behind a unit test that skipped and printed ok."""
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": SKIP},
             go_covers={"TestSinkClickhouse_InsertsRows": ["sink.clickhouse"]})

    assert level(s, "sink.clickhouse", "unit")["status"] == "skipped"
    assert {"feature": "sink.clickhouse", "level": "unit",
            "status": "skipped"} in s["gaps"]


def test_one_passing_test_covers_a_feature_others_skipped():
    s = snap(go_results={
        "TestSinkClickhouse_InsertsRows": PASS,
        "TestSinkClickhouse_InsertsArrays": SKIP,
    }, go_covers={
        "TestSinkClickhouse_InsertsRows": ["sink.clickhouse"],
        "TestSinkClickhouse_InsertsArrays": ["sink.clickhouse"],
    })
    assert level(s, "sink.clickhouse", "unit")["status"] == "covered"


def test_a_failing_test_does_not_cover_its_feature():
    s = snap(go_results={
        "TestSinkClickhouse_InsertsRows": PASS,
        "TestSinkClickhouse_InsertsArrays": FAIL,
    }, go_covers={
        "TestSinkClickhouse_InsertsRows": ["sink.clickhouse"],
        "TestSinkClickhouse_InsertsArrays": ["sink.clickhouse"],
    })
    assert level(s, "sink.clickhouse", "unit")["status"] == "failing"


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
    s = snap(py_results={"test_sink_console_writes_rows": PASS},
             py_covers={"test_sink_console_writes_rows": ["sink.console"]})
    assert level(s, "sink.console", "release")["status"] == "covered"


def test_an_integration_result_lands_at_the_integration_level():
    s = snap(
        integration_results={"TestIntegrationSourceKafka_Commits": PASS},
        integration_covers={"TestIntegrationSourceKafka_Commits": ["source.kafka"]},
        features=[{"id": "source.kafka", "description": "kafka",
                   "requires": ["integration"]}],
    )
    assert level(s, "source.kafka", "integration")["status"] == "covered"


def test_an_integration_test_does_not_manufacture_unit_coverage():
    """The same names appear in the unit pass, skipped by -short. A skip is not
    coverage there just because the integration pass ran it for real."""
    s = snap(
        go_results={"TestIntegrationSourceKafka_Commits": SKIP},
        go_covers={"TestIntegrationSourceKafka_Commits": ["source.kafka"]},
        integration_results={"TestIntegrationSourceKafka_Commits": PASS},
        integration_covers={"TestIntegrationSourceKafka_Commits": ["source.kafka"]},
        features=[{"id": "source.kafka", "description": "kafka",
                   "requires": ["unit", "integration"]}],
    )
    assert level(s, "source.kafka", "unit")["status"] == "skipped"
    assert level(s, "source.kafka", "integration")["status"] == "covered"


def test_a_skipped_integration_test_is_a_gap():
    """An integration suite that skips itself when its service is absent is the
    sink.iceberg failure one level up. It must not read as coverage."""
    s = snap(
        integration_results={"TestIntegrationSourceKafka_Commits": SKIP},
        integration_covers={"TestIntegrationSourceKafka_Commits": ["source.kafka"]},
        features=[{"id": "source.kafka", "description": "kafka",
                   "requires": ["integration"]}],
    )
    assert {"feature": "source.kafka", "level": "integration",
            "status": "skipped"} in s["gaps"]


def test_a_marker_attributes_a_second_feature():
    s = snap(
        py_results={"test_handler_inferred_mem_aggregates": PASS},
        py_covers={"test_handler_inferred_mem_aggregates": ["source.kafka"]},
        features=FEATURES + [{"id": "handler.inferred_mem", "description": "h",
                              "requires": ["release"]}],
    )
    assert level(s, "source.kafka", "release")["status"] == "covered"


def test_a_marker_is_recorded_as_secondary_attribution():
    """A test may claim more than one feature. The first is what it is for;
    the rest are what it proves in passing, and the page names those."""
    s = snap(
        py_results={"test_handler_inferred_mem_aggregates": PASS},
        py_covers={"test_handler_inferred_mem_aggregates":
                   ["handler.inferred_mem", "source.kafka"]},
        features=FEATURES + [{"id": "handler.inferred_mem", "description": "h",
                              "requires": ["release"]}],
    )
    lvl = level(s, "source.kafka", "release")
    assert lvl["by_marker"] == ["test_handler_inferred_mem_aggregates"]


def test_a_marker_carries_the_outcome_not_just_the_name():
    """A marker on a skipped test must not manufacture coverage."""
    s = snap(
        py_results={"test_handler_inferred_mem_aggregates": SKIP},
        py_covers={"test_handler_inferred_mem_aggregates": ["source.kafka"]},
        features=FEATURES + [{"id": "handler.inferred_mem", "description": "h",
                              "requires": ["release"]}],
    )
    assert level(s, "source.kafka", "release")["status"] == "skipped"


def test_a_marker_naming_an_undeclared_feature_is_reported():
    s = snap(
        go_results={"TestSinkClickhouse_InsertsRows": PASS},
        go_covers={"TestSinkClickhouse_InsertsRows": ["sink.nonexistent"]},
    )
    assert {"test": "TestSinkClickhouse_InsertsRows",
            "feature": "sink.nonexistent"} in s["unknown_markers"]


def test_a_marker_for_the_feature_the_name_already_claims_is_not_doubled():
    s = snap(
        go_results={"TestSinkClickhouse_InsertsRows": PASS},
        go_covers={"TestSinkClickhouse_InsertsRows": ["sink.clickhouse"]},
    )
    assert level(s, "sink.clickhouse", "unit")["tests"] == [
        "TestSinkClickhouse_InsertsRows"]


def test_the_snapshot_is_deterministic():
    """CI diffs this file. Unstable ordering would make every run a diff."""
    args = dict(
        go_results={"TestSinkClickhouse_B": PASS,
                    "TestSinkClickhouse_A": PASS,
                    "TestStateDurability_X": PASS},
        py_results={"test_source_kafka_y": PASS},
    )
    first = json.dumps(snap(**args), indent=2)
    second = json.dumps(snap(**args), indent=2)
    assert first == second


def test_the_snapshot_orders_tests_stably():
    s = snap(go_results={"TestSinkClickhouse_B": PASS,
                         "TestSinkClickhouse_A": PASS})
    names = level(s, "sink.clickhouse", "unit")["tests"]
    assert names == sorted(names)


def test_a_test_is_recorded_as_a_bare_name():
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": PASS},
             go_covers={"TestSinkClickhouse_InsertsRows": ["sink.clickhouse"]})
    assert level(s, "sink.clickhouse", "unit")["tests"] == [
        "TestSinkClickhouse_InsertsRows"]


def test_a_level_with_nothing_exceptional_carries_only_status_and_tests():
    """The common case pays for nothing it does not use."""
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": PASS})
    assert set(level(s, "sink.clickhouse", "unit")) == {"status", "tests"}


def test_an_empty_list_is_omitted_entirely():
    s = snap()
    assert level(s, "sink.console", "release") == {"status": "not_required"}


def test_a_skipped_test_is_named_in_the_skipped_list():
    """Status alone cannot say which of several tests skipped."""
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": PASS,
                         "TestSinkClickhouse_InsertsArrays": SKIP},
             go_covers={"TestSinkClickhouse_InsertsRows": ["sink.clickhouse"],
                        "TestSinkClickhouse_InsertsArrays": ["sink.clickhouse"]})
    lvl = level(s, "sink.clickhouse", "unit")

    assert lvl["status"] == "covered"
    assert lvl["skipped"] == ["TestSinkClickhouse_InsertsArrays"]
    assert "failing" not in lvl


def test_a_failing_test_is_named_in_the_failing_list():
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": PASS,
                         "TestSinkClickhouse_InsertsArrays": FAIL},
             go_covers={"TestSinkClickhouse_InsertsRows": ["sink.clickhouse"],
                        "TestSinkClickhouse_InsertsArrays": ["sink.clickhouse"]})
    lvl = level(s, "sink.clickhouse", "unit")

    assert lvl["failing"] == ["TestSinkClickhouse_InsertsArrays"]
    assert "skipped" not in lvl


def test_every_exceptional_name_also_appears_in_tests():
    """The lists narrow the test list. They never extend it."""
    s = snap(go_results={"TestSinkClickhouse_A": PASS,
                         "TestSinkClickhouse_B": SKIP,
                         "TestSinkClickhouse_C": FAIL})
    lvl = level(s, "sink.clickhouse", "unit")

    for key in ("skipped", "failing", "by_marker"):
        assert set(lvl.get(key, [])) <= set(lvl["tests"]), key


def test_the_snapshot_declares_its_schema_version():
    """A reader that expects per-test objects must be able to tell."""
    assert snap()["version"] == 3


def test_feature_gaps_reads_only_status_and_requires():
    entries = [{
        "id": "sink.clickhouse", "requires": ["unit", "release"],
        "levels": {"unit": {"status": "covered"},
                   "integration": {"status": "not_required"},
                   "release": {"status": "skipped"}},
    }]
    assert feature_gaps(entries) == [
        {"feature": "sink.clickhouse", "level": "release", "status": "skipped"}]


def test_a_test_carrying_any_marker_is_not_unattributed():
    """A marker-attributed test was listed under "match no declared feature".
    The conformance harness attributes only by marker, and a shared runner's
    name matches no feature prefix at all, so every one of its cases would
    land there telling the reader to rename a test that already says what it
    covers."""
    name = "TestConformanceSinks_KeepsBatch"
    s = snap(
        integration_results={name: PASS},
        integration_covers={name: ["sink.clickhouse"]},
    )
    assert s["unattributed"]["integration"] == []
    assert level(s, "sink.clickhouse", "integration")["status"] == "covered"


def test_a_test_matching_nothing_and_carrying_no_marker_is_still_unattributed():
    """The report still catches what it is for: #221's four misnamed tests."""
    s = snap(integration_results={"TestNobodyDeclaredThis": PASS})
    assert s["unattributed"]["integration"] == ["TestNobodyDeclaredThis"]


def test_a_test_with_neither_marker_nor_matching_name_is_still_unattributed():
    coverage, secondary, unmatched, unknown = build(
        FEATURES, {}, {}, {}, {}, {"TestNobodyDeclaredThis": PASS}, {})
    s = snapshot(FEATURES, coverage, secondary, unmatched, unknown)
    assert s["unattributed"]["integration"] == ["TestNobodyDeclaredThis"]


def test_a_skipped_test_still_reports_its_feature():
    """A marker must fire before any early return. Under name attribution a
    test kept its feature even when it skipped; a marker placed after a
    `-short` guard fires never, and the test covers nothing.

    The integration tests skip in the unit pass, so this is every one of
    them."""
    s = snap(
        go_results={"TestIntegrationSinkClickhouse_Conformance": SKIP},
        go_covers={"TestIntegrationSinkClickhouse_Conformance": ["sink.clickhouse"]},
    )
    cell = level(s, "sink.clickhouse", "unit")

    assert cell["status"] == "skipped"
    assert cell["skipped"] == ["TestIntegrationSinkClickhouse_Conformance"]
    assert s["unattributed"]["unit"] == []


def test_a_feature_claimed_twice_by_one_test_counts_once():
    """A test can reach the same marker twice: the conformance entry points
    emit one before the -short skip and one when the harness finishes. Counting
    both inflates the Tests column and puts the same name in a cell twice."""
    s = snap(
        go_results={"TestX": PASS},
        go_covers={"TestX": ["sink.clickhouse", "sink.clickhouse"]},
    )
    assert level(s, "sink.clickhouse", "unit")["tests"] == ["TestX"]