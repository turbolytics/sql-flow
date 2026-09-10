"""Tests for coverage_matrix.suites."""

import json

from coverage_matrix.registries import FAIL, PASS, SKIP
from coverage_matrix.suites import parse_go, parse_pytest
from samples import *  # noqa: F401,F403


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
    results, covers, _ = parse_go(path)

    assert results == {"TestA": PASS, "TestB": SKIP, "TestC": FAIL}
    assert covers == {"TestA": ["sink.clickhouse"]}


def test_parse_go_ignores_package_level_events(tmp_path):
    """Events with no Test are the package summary, not a test result."""
    path = write(tmp_path, "go.json",
                 json.dumps({"Action": "pass", "Package": "x"}))
    results, _, _ = parse_go(path)
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
    results, covers, invariants = parse_go(path)

    assert results == {"TestA": PASS}
    assert covers == {"TestA": ["sink.clickhouse"]}
    assert invariants == {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}


def test_parse_pytest_returns_an_empty_invariant_map(tmp_path):
    path = write(tmp_path, "pytest.json", json.dumps({"tests": [
        {"nodeid": "test_a", "outcome": "passed", "covers": []},
    ]}))
    results, covers, invariants = parse_pytest(path)
    assert invariants == {}


def test_parse_pytest_reads_outcomes_and_markers(tmp_path):
    path = write(tmp_path, "pytest.json", json.dumps({"tests": [
        {"nodeid": "test_sink_clickhouse_x", "outcome": "passed",
         "covers": ["source.kafka"]},
        {"nodeid": "test_b", "outcome": "skipped", "covers": []},
        {"nodeid": "test_c", "outcome": "failed", "covers": []},
    ]}))
    results, covers, _ = parse_pytest(path)

    assert results == {"test_sink_clickhouse_x": PASS,
                       "test_b": SKIP, "test_c": FAIL}
    assert covers == {"test_sink_clickhouse_x": ["source.kafka"]}