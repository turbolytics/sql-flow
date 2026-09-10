"""Tests for coverage_matrix.report."""

from coverage_matrix.registries import PASS
from coverage_matrix.report import (
    domain, least_proven_invariant, most_tested_feature, render_report,
    strongest_argument)
from samples import *  # noqa: F401,F403


def test_the_argument_pairs_a_feature_and_an_invariant_of_the_same_layer():
    """A heavily tested feature elsewhere in the engine says nothing about a
    sink invariant. Only same-layer pairs are an argument."""
    features = FEATURES + [
        {"id": "config.validation", "description": "c", "requires": ["unit"]},
        {"id": "sink.retry", "description": "r", "requires": ["unit"]},
    ]
    s = snap(features=features, go_results={
        **{f"TestConfigValidation_{i}": PASS for i in range(20)},
        **{f"TestSinkRetry_{i}": PASS for i in range(5)},
    }, go_covers={
        **{f"TestConfigValidation_{i}": ["config.validation"] for i in range(20)},
        **{f"TestSinkRetry_{i}": ["sink.retry"] for i in range(5)},
    })
    s.update(inv_snap())

    feature, tests, invariant, _, _ = strongest_argument(s)
    # config.validation has four times the tests and no invariant of its own,
    # so it is not the argument.
    assert feature == "sink.retry"
    assert tests == 5
    assert invariant.startswith("sink.")


def test_the_argument_needs_an_invariant_in_the_same_layer():
    """A layer with tests and no invariants declared is not an argument."""
    features = [{"id": "config.validation", "description": "c", "requires": ["unit"]}]
    s = snap(features=features,
             go_results={"TestConfigValidation_A": PASS},
             go_covers={"TestConfigValidation_A": ["config.validation"]})
    s.update(inv_snap())
    assert strongest_argument(s) is None


def test_domain_names_the_subsystem():
    assert domain("sink.retry") == "sink"
    assert domain("sink.flush.keeps_batch") == "sink"
    assert domain("config.validation") == "config"


def test_the_most_tested_feature_counts_across_every_level():
    s = snap(
        go_results={"TestSinkClickhouse_A": PASS},
        go_covers={"TestSinkClickhouse_A": ["sink.clickhouse"]},
        py_results={"test_sink_clickhouse_b": PASS},
        py_covers={"test_sink_clickhouse_b": ["sink.clickhouse"]},
        integration_results={"TestSinkConsole_C": PASS},
        integration_covers={"TestSinkConsole_C": ["sink.console"]},
    )
    assert most_tested_feature(s) == ("sink.clickhouse", 2)


def test_the_least_proven_invariant_ignores_exempt_integrations():
    """console is exempt from keeps_batch, so it is neither proof nor a hole:
    counting it as unproven would overstate the gap."""
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}})

    invariant, proven, applicable, _ = least_proven_invariant(s)
    # only_processed applies to source.kafka alone and has no proof at all.
    assert invariant == "source.commit.only_processed"
    assert (proven, applicable) == (0, 1)


def test_the_least_proven_invariant_prefers_the_smaller_share():
    """Two invariants with no proof and different reach: the one covering more
    integrations is the bigger hole, but share is what ranks them."""
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}})
    _, proven, applicable, share = least_proven_invariant(s)
    assert share == proven / applicable


def test_the_least_proven_invariant_is_none_when_nothing_is_applicable():
    """Nothing to prove is not the same as nothing proven, and the page must
    say nothing rather than invent a worst case."""
    integrations = [dict(INTEGRATIONS[1])]  # console only, and it is exempt
    s = inv_snap(integrations=integrations)
    assert least_proven_invariant(s) is None


def test_the_report_counts_the_tests_rather_than_sampling_them():
    """The count answers "is this feature thinly covered"; .coverage/matrix.json
    names them all."""
    s = snap(go_results={f"TestSinkClickhouse_{i}": PASS for i in range(4)},
             go_covers={f"TestSinkClickhouse_{i}": ["sink.clickhouse"]
                        for i in range(4)})
    row = next(line for line in render_report(s).splitlines()
               if line.startswith("| `sink.clickhouse`"))

    assert row == "| `sink.clickhouse` | 4 | — | — | 4 |"
    assert "TestSinkClickhouse_0" not in row


def test_the_report_leaves_the_count_blank_when_nothing_covers_the_feature():
    row = next(line for line in render_report(snap()).splitlines()
               if line.startswith("| `sink.console`"))
    assert row == "| `sink.console` | — | — | — | — |"


def test_the_report_names_a_feature_covered_only_by_a_marker():
    s = snap(
        py_results={"test_handler_inferred_mem_aggregates": PASS},
        py_covers={"test_handler_inferred_mem_aggregates":
                   ["handler.inferred_mem", "source.kafka"]},
        features=FEATURES + [{"id": "handler.inferred_mem", "description": "h",
                              "requires": ["release"]}],
    )
    out = render_report(s)
    assert "Covered only by another test's marker" in out
    assert "`source.kafka` (release)" in out


def test_an_unattributed_test_reaches_the_report():
    out = render_report(snap(go_results={"TestNobodyDeclaredThis": PASS}))
    assert "## Unattributed unit tests (1)" in out
    assert "- `TestNobodyDeclaredThis`" in out


def test_the_report_names_an_unknown_marker_and_says_it_fails_the_gate():
    s = snap(go_results={"TestSinkClickhouse_InsertsRows": PASS},
             go_covers={"TestSinkClickhouse_InsertsRows": ["sink.nonexistent"]})
    s.update(inv_snap(
        evidence={"unit": {"TestX": [("sink.flush.bogus", "sink.clickhouse")]}},
        results={"unit": {"TestX": PASS}}))
    out = render_report(s)
    assert "`TestSinkClickhouse_InsertsRows` marks `sink.nonexistent`" in out
    assert "`TestX` marks `sink.flush.bogus` on `sink.clickhouse`" in out
    assert "fail" in out.split("## Markers naming an unknown feature")[1]


def test_the_report_pairs_the_most_tested_feature_with_the_least_proven_invariant():
    """The argument for the invariant matrix, generated so it cannot go
    stale. A feature can carry dozens of tests while the invariant those
    tests depend on is proven almost nowhere."""
    s = snap(go_results={
        "TestSinkClickhouse_A": PASS,
        "TestSinkClickhouse_B": PASS,
        "TestSinkConsole_C": PASS,
    }, go_covers={
        "TestSinkClickhouse_A": ["sink.clickhouse"],
        "TestSinkClickhouse_B": ["sink.clickhouse"],
        "TestSinkConsole_C": ["sink.console"],
    })
    s.update(inv_snap())
    out = render_report(s)

    assert "## Why invariants, and not the test count" in out
    assert "`sink.clickhouse` carries 2 attributed tests" in out
    assert "is proven on 0 of the" in out


def test_the_argument_is_omitted_when_no_feature_has_a_test():
    s = snap()
    s.update(inv_snap())
    assert "## Why invariants, and not the test count" not in render_report(s)


def test_the_argument_is_omitted_when_no_invariant_is_applicable():
    s = snap(go_results={"TestSinkClickhouse_A": PASS})
    s.update(inv_snap(integrations=[dict(INTEGRATIONS[1])]))
    assert "## Why invariants, and not the test count" not in render_report(s)


def test_the_report_works_on_a_feature_only_snapshot():
    assert "# Coverage report" in render_report(snap())