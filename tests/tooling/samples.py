"""Sample registries and the builders over them, shared by the module tests.

Importable because tests/tooling carries no __init__.py, so pytest puts the
directory on sys.path; conftest.py puts the generator there.
"""

from coverage_matrix.features import build, snapshot
from coverage_matrix.invariants import build_invariants, snapshot_invariants
from coverage_matrix.page import page_snapshot
from coverage_matrix.registries import (
    FAIL, LEVELS, PASS, SKIP, STATUS_DIR, load_features, load_integrations,
    load_invariants)
from coverage_matrix.statusfiles import read_status, status_from_snapshot


FEATURES = [
    {"id": "state.durability", "description": "state", "requires": ["unit"]},
    {"id": "sink.clickhouse", "description": "ch", "requires": ["unit", "release"]},
    {"id": "sink.console", "description": "console", "requires": ["unit"]},
    {"id": "source.kafka", "description": "kafka", "requires": ["release"]},
    {"id": "state.durability", "description": "state", "requires": ["unit"]},
]

def snap(go_results=None, py_results=None, go_covers=None, py_covers=None,
         integration_results=None, integration_covers=None, features=None):
    features = features or FEATURES
    coverage, secondary, unmatched, unknown = build(
        features, go_results or {}, py_results or {},
        go_covers or {}, py_covers or {},
        integration_results or {}, integration_covers or {})
    return snapshot(features, coverage, secondary, unmatched, unknown)

def level(s, feature_id, lvl):
    for feature in s["features"]:
        if feature["id"] == feature_id:
            return feature["levels"][lvl]
    raise AssertionError(f"{feature_id} not in the snapshot")

def write(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text)
    return str(path)

INVARIANTS = [
    {"id": "sink.flush.keeps_batch", "family": "resilience", "class": "safety",
     "applies_to": "sink", "claim": "kept", "verified_by": "harness",
     "requires": []},
    {"id": "source.commit.only_processed", "family": "checkpoint",
     "class": "safety", "applies_to": "source", "claim": "c",
     "verified_by": "harness", "requires": []},
]

INTEGRATIONS = [
    {"id": "sink.clickhouse", "kind": "sink", "implements": ["Sink"],
     "feature": "sink.clickhouse", "exempt": []},
    {"id": "sink.console", "kind": "sink", "implements": ["Sink"],
     "feature": "sink.console",
     "exempt": [{"invariant": "sink.flush.keeps_batch", "reason": "stdout",
                 "proven_by": "TestSinkConsole_ImplementsNoProber"}]},
    {"id": "source.kafka", "kind": "source", "implements": ["Source"],
     "feature": "source.kafka", "exempt": []},
    {"id": "pipeline.stateful", "kind": "pipeline", "constructed": False,
     "implements": ["Sink"], "feature": "state.durability", "exempt": []},
]

def inv_snap(evidence=None, results=None, invariants=None, integrations=None):
    """evidence: {level: {test: [(invariant, integration)]}}
    results:    {level: {test: outcome}}"""
    evidence = evidence or {}
    results = results or {}
    invariants = invariants or INVARIANTS
    integrations = integrations or INTEGRATIONS
    built = build_invariants(
        invariants, integrations,
        {lvl: results.get(lvl, {}) for lvl in LEVELS},
        {lvl: evidence.get(lvl, {}) for lvl in LEVELS})
    return snapshot_invariants(invariants, integrations, built)

def cell(s, invariant, integration, lvl):
    for inv in s["invariants"]:
        if inv["id"] == invariant:
            return inv["integrations"][integration][lvl]
    raise AssertionError(f"{invariant} not in the snapshot")

def built(evidence=None, results=None, invariants=None, integrations=None):
    evidence = evidence or {}
    results = results or {}
    return build_invariants(
        invariants or INVARIANTS, integrations or INTEGRATIONS,
        {lvl: results.get(lvl, {}) for lvl in LEVELS},
        {lvl: evidence.get(lvl, {}) for lvl in LEVELS})

def levels(**states):
    return {lvl: {"status": states.get(lvl, "missing")} for lvl in LEVELS}

def committed_page_snapshot():
    return page_snapshot(load_features(), load_invariants(),
                            load_integrations(), read_status(STATUS_DIR))

PIPELINE = [
    {"id": "pipeline.commit.after_flush", "family": "checkpoint",
     "class": "safety", "applies_to": "pipeline", "claim": "c",
     "verified_by": "harness", "requires": []},
]

SAFETY = dict(INVARIANTS[0], **{"class": "safety"})

LIVENESS = {"id": "pipeline.flush.eventually", "family": "lifecycle",
            "class": "liveness", "applies_to": "pipeline",
            "claim": "buffered rows reach the sink within the flush interval",
            "verified_by": "harness", "requires": []}

LATTICE = [
    {"key": "int64", "duckdb": ["BIGINT"], "depth": 1},
    {"key": "utf8", "duckdb": ["VARCHAR"], "depth": 1},
]

def typed(types):
    return [{"id": "sink.clickhouse", "kind": "sink",
             "feature": "sink.clickhouse", "types": types}]

def status_of(**kwargs):
    s = snap(**kwargs)
    s.update(inv_snap())
    return status_from_snapshot(s)

def page_of(status=None, features=None, invariants=None, integrations=None):
    return page_snapshot(features or FEATURES, invariants or INVARIANTS,
                            integrations or INTEGRATIONS,
                            status or {"features": {}, "integrations": {}})