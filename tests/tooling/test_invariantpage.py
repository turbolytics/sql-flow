"""Tests for coverage_matrix.invariantpage."""

from coverage_matrix.invariantpage import invariant_cell, render_invariants
from coverage_matrix.registries import LEVELS, PASS
from samples import *  # noqa: F401,F403


def test_render_carries_one_table_per_family():
    s = inv_snap()
    md = render_invariants(s)
    assert "## Safety invariants: resilience" in md
    assert "## Safety invariants: checkpoint" in md
    assert "| `sink.flush.keeps_batch` |" in md


def test_render_marks_an_exempt_cell():
    s = inv_snap()
    md = render_invariants(s)
    line = next(l for l in md.splitlines() if "`sink.flush.keeps_batch`" in l)
    assert "exempt" in line


def test_render_agrees_with_the_snapshot():
    """The markdown is a view. It can never disagree with the JSON that gates
    the build, and a cell the JSON calls covered must not read as missing."""
    s = inv_snap(
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse",
                "unit")["status"] == "covered"

    md = render_invariants(s)
    line = next(l for l in md.splitlines() if "`sink.flush.keeps_batch`" in l)
    assert "✅ u" in line


def test_render_shows_covered_when_every_judged_level_is_covered():
    required = [dict(INVARIANTS[0], requires=["unit"])]
    s = inv_snap(
        invariants=required,
        evidence={"unit": {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"unit": {"TestA": PASS}},
    )
    md = render_invariants(s)
    line = next(l for l in md.splitlines() if "`sink.flush.keeps_batch`" in l)
    assert "✅ u" in line


def test_cell_names_the_level_that_proved_it():
    assert invariant_cell(levels(integration="covered"), []) == "✅ i"
    assert invariant_cell(levels(unit="covered"), []) == "✅ u"


def test_cell_names_every_level_that_proved_it():
    """'covered at unit but never against the real thing' is the question the
    matrix exists to answer, so both levels show."""
    assert invariant_cell(
        levels(unit="covered", integration="covered"), []) == "✅ ui"


def test_cell_with_no_evidence_is_missing():
    assert invariant_cell(levels(), []) == "❌ missing"


def test_cell_reports_a_failure_over_anything_achieved():
    assert invariant_cell(
        levels(unit="covered", integration="failing"), []) == "🔥 failing"


def test_cell_reports_a_skip_rather_than_missing():
    assert invariant_cell(levels(unit="skipped"), []) == "⚠️ skipped"


def test_cell_is_exempt_whatever_else_is_present():
    assert invariant_cell(
        {lvl: {"status": "exempt", "reason": "r"} for lvl in LEVELS},
        []) == "— exempt"


def test_cell_warns_when_a_required_level_is_not_the_one_proven():
    """A tick beside an unmet requirement reads as done. The gap is listed
    below the table, and the cell must not contradict it."""
    assert invariant_cell(
        levels(unit="covered"), ["integration"]) == "⚠️ u, integration missing"


def test_render_names_the_tracking_issue_of_an_unenforced_invariant():
    s = inv_snap(invariants=[dict(INVARIANTS[0], tracked_by="#183")])
    md = render_invariants(s)
    assert "tracked by #183" in md


def test_render_lists_a_gap():
    required = [dict(INVARIANTS[0], requires=["unit"])]
    md = render_invariants(inv_snap(invariants=required))
    assert "## Invariant gaps" in md
    assert "`sink.flush.keeps_batch` on `sink.clickhouse` requires **unit**" in md


def test_render_omits_the_gap_section_when_there_are_none():
    assert "## Invariant gaps" not in render_invariants(inv_snap())


def test_render_gives_the_pipeline_table_a_column_per_configuration():
    """A pipeline row among sink columns used to render as a line of dots,
    which reads as "not applicable" when the truth is "unproven"."""
    md = render_invariants(inv_snap(invariants=PIPELINE))

    assert "| Invariant | Claim | `pipeline.stateful` |" in md
    assert "| `pipeline.commit.after_flush` | c | ❌ missing |" in md


def test_render_shows_a_pipeline_invariant_as_covered_once_proven():
    s = inv_snap(
        invariants=PIPELINE,
        evidence={"unit": {"TestA": [("pipeline.commit.after_flush",
                                      "pipeline.stateful")]}},
        results={"unit": {"TestA": PASS}},
    )
    assert "| `pipeline.commit.after_flush` | c | ✅ u |" in render_invariants(s)


def test_render_separates_pipeline_rows_from_integration_rows_in_one_family():
    """checkpoint holds both. Mixing them puts dots in a table of real cells."""
    mixed = [
        dict(INVARIANTS[1], family="checkpoint"),           # applies_to source
        PIPELINE[0],
    ]
    md = render_invariants(inv_snap(invariants=mixed))

    assert md.count("## Safety invariants: checkpoint") == 1
    assert "| Invariant | Claim | `source.kafka` |" in md
    assert "| Invariant | Claim | `pipeline.stateful` |" in md
    # The pipeline row never appears in the source table.
    source_table = md.split("| Invariant | Claim | `pipeline.stateful` |")[0]
    assert "pipeline.commit.after_flush" not in source_table


def test_a_claim_carries_the_defect_that_proves_it_matters():
    """violated_once is the difference between a rule and a scar."""
    s = inv_snap(invariants=[dict(INVARIANTS[0], violated_once=["#221"])])
    assert "violated once: #221" in render_invariants(s)


def test_render_groups_tables_by_class_then_family():
    """Grouping rather than a column, so a liveness section that is entirely
    red is visible beside a green safety one."""
    s = inv_snap(invariants=[SAFETY, LIVENESS])
    md = render_invariants(s)

    assert "## Safety invariants: resilience" in md
    assert "## Liveness invariants: lifecycle" in md