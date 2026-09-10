"""Tests for coverage_matrix.page."""

from coverage_matrix.invariantpage import render_invariants
from coverage_matrix.page import render, render_page
from coverage_matrix.registries import (
    MATRIX_MD, PASS, SKIP, STATUS_DIR, load_features, load_integrations,
    load_invariants)
from coverage_matrix.statusfiles import read_status
from samples import *  # noqa: F401,F403


def test_render_carries_a_column_per_level():
    s = snap(integration_results={"TestSourceKafka_Commits": PASS})
    assert "| Feature | What it does | unit | integration | release |" in render(s)
    assert "| Tests |" not in render(s)


def test_render_agrees_with_the_snapshot():
    """The markdown is a view. It must not be able to disagree with the JSON
    that gates the build."""
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": SKIP})
    out = render(s)

    assert "sink.clickhouse" in out
    assert "skipped" in out
    assert "1 gap(s)" in out or "gap(s)" in out


def test_the_feature_total_says_it_counts_attribution_not_proof():
    """"33 fully covered" beside an almost empty invariant matrix reads as
    proof. The reader who stops at the first bold line must not be misled."""
    md = render(snap(go_results={"TestSinkClickhouse_InsertsRows": PASS}))

    assert "at least one passing test attributed at every level they require" in md
    assert "counts attribution, not proof" in md


def test_the_feature_total_omits_the_invariant_count_when_there_is_none():
    """render() is called on feature-only snapshots in tests and by nothing
    else; it must not require the second axis."""
    md = render(snap())
    assert "invariants declared" not in md


def test_the_invariant_count_sits_beside_the_feature_count():
    s = snap()
    s.update(inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}}))
    md = render(s)

    assert "invariants declared" in md
    assert "1 proven" in md
    assert "exempt" in md


def test_the_committed_page_is_current():
    """The page is a function of committed files, so a stale one is caught
    here, in seconds, before any suite runs. Regenerate with
    `make coverage-page`."""
    expected = render_page(load_features(), load_invariants(),
                              load_integrations(), read_status(STATUS_DIR))
    with open(MATRIX_MD) as fh:
        assert fh.read() == expected


def test_render_counts_each_class_separately():
    """Beside the feature count, because a reader who stops at the first bold
    line should see both numbers."""
    s = snap()
    s.update(inv_snap(invariants=[SAFETY, LIVENESS]))
    md = render(s)
    assert "1 safety" in md and "1 liveness" in md


def test_render_says_what_safety_alone_cannot_prove():
    """The vacuity is the reason the distinction exists, and a reader cannot
    infer it from a page of green cells."""
    s = snap()
    s.update(inv_snap(invariants=[SAFETY, LIVENESS]))
    assert "never flushes" in render(s)


def test_the_page_snapshot_reads_a_feature_status_from_the_files():
    ps = page_of(status={"features": {"sink.clickhouse": {
        "unit": "covered", "integration": "not_required", "release": "skipped"}},
        "integrations": {}})
    assert level(ps, "sink.clickhouse", "release")["status"] == "skipped"
    assert {"feature": "sink.clickhouse", "level": "release",
            "status": "skipped"} in ps["gaps"]


def test_a_feature_absent_from_the_status_is_missing_where_required():
    """A feature added to the registry before the next generation renders
    honestly rather than crashing the page."""
    ps = page_of()
    assert level(ps, "sink.clickhouse", "unit")["status"] == "missing"
    assert level(ps, "sink.clickhouse", "integration")["status"] == "not_required"


def test_the_page_snapshot_reads_an_invariant_cell_from_the_files():
    ps = page_of(status={"features": {}, "integrations": {
        "sink.clickhouse": {"sink.flush.keeps_batch": {
            "unit": "covered", "integration": "missing", "release": "missing"}}}})
    assert cell(ps, "sink.flush.keeps_batch", "sink.clickhouse", "unit")["status"] == "covered"


def test_the_page_renders_from_status_and_registries_with_no_test_report():
    md = render_page(FEATURES, INVARIANTS, INTEGRATIONS, {
        "features": {"sink.clickhouse": {
            "unit": "covered", "integration": "not_required", "release": "covered"}},
        "integrations": {"sink.clickhouse": {"sink.flush.keeps_batch": {
            "unit": "covered", "integration": "missing", "release": "missing"}}},
    })
    row = next(l for l in md.splitlines() if l.startswith("| `sink.clickhouse`"))
    assert row == "| `sink.clickhouse` | ch | ✅ | — | ✅ |"
    line = next(l for l in md.splitlines() if "`sink.flush.keeps_batch`" in l)
    assert "✅ u" in line


def test_the_page_says_where_the_names_went():
    """Dropping the names without saying where they went sends the reader
    to grep the repo."""
    md = render(snap())
    assert "report" in md
    assert "names every one of them" not in md


def test_the_page_carries_no_test_name():
    s = snap(go_results={"TestSinkClickhouse_A": PASS,
                         "TestNobodyDeclaredThis": PASS},
             go_covers={"TestSinkClickhouse_A": ["sink.clickhouse", "sink.console"]})
    s.update(inv_snap(
        evidence={"unit": {"TestHarness_X": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestHarness_X": PASS}}))
    md = render(s) + render_invariants(s)
    for name in ("TestSinkClickhouse_A", "TestNobodyDeclaredThis", "TestHarness_X"):
        assert name not in md, name
    assert "Unattributed" not in md
    assert "Covered only by another test's marker" not in md