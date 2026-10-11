"""Tests for coverage_matrix.types."""

from coverage_matrix.registries import load_integrations, load_lattice
from coverage_matrix.types import render_types_page, validate_types
from samples import *  # noqa: F401,F403


def test_a_type_key_outside_the_lattice_is_reported():
    problems = validate_types(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": [{"type": "Int64", "expect": "v"}]},
        "utf8": {"outcome": "exact", "columns": [{"type": "String", "expect": "v"}]},
        "decimal128(38, 0)": {"outcome": "unsupported",
                              "code": "user.sink.type_unsupported"},
    }))
    assert any("decimal128(38, 0)" in p and "lattice.yml" in p for p in problems)


def test_a_lattice_key_the_sink_does_not_declare_is_reported():
    problems = validate_types(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": [{"type": "Int64", "expect": "v"}]},
    }))
    assert any("utf8" in p and "sink.clickhouse" in p for p in problems)


def test_a_sink_with_no_type_table_is_not_reported_key_by_key():
    """One sink has a table in this revision. Five do not, and 29 lines each
    would bury the one that does."""
    problems = validate_types(
        LATTICE, [{"id": "sink.kafka", "kind": "sink", "feature": "sink.kafka"}])
    assert problems == []


def test_a_coerced_row_without_a_rule_is_reported():
    """A coercion stated as an outcome and no rule is an excuse. The rule is
    what the integration page publishes."""
    problems = validate_types(LATTICE, typed({
        "int64": {"outcome": "coerced", "columns": [{"type": "Int64", "expect": "v"}]},
        "utf8": {"outcome": "exact", "columns": [{"type": "String", "expect": "v"}]},
    }))
    assert any("int64" in p and "rule" in p for p in problems)


def test_an_unsupported_row_without_a_code_is_reported():
    problems = validate_types(LATTICE, typed({
        "int64": {"outcome": "unsupported"},
        "utf8": {"outcome": "exact", "columns": [{"type": "String", "expect": "v"}]},
    }))
    assert any("int64" in p and "code" in p for p in problems)


def test_a_supported_row_naming_no_column_is_reported():
    """Nothing to write it to means nothing was proven."""
    problems = validate_types(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": []},
        "utf8": {"outcome": "exact", "columns": [{"type": "String", "expect": "v"}]},
    }))
    assert any("int64" in p and "column" in p for p in problems)


def test_an_unknown_outcome_is_reported():
    problems = validate_types(LATTICE, typed({
        "int64": {"outcome": "probably fine", "columns": [{"type": "Int64", "expect": "v"}]},
        "utf8": {"outcome": "exact", "columns": [{"type": "String", "expect": "v"}]},
    }))
    assert any("probably fine" in p for p in problems)


def test_a_consistent_table_reports_nothing():
    problems = validate_types(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": [{"type": "Int64", "expect": "v"}]},
        "utf8": {"outcome": "unsupported", "code": "user.sink.type_unsupported"},
    }))
    assert problems == []


def test_the_real_registries_agree():
    """The check runs against the shipped files, not only fixtures. A gap here
    is a real gap."""
    assert validate_types(load_lattice(), load_integrations()) == []


def test_the_page_is_keyed_by_duckdb_type_not_arrow():
    """A user writes a CAST and never sees an Arrow type."""
    md = render_types_page(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": [{"type": "Int64", "expect": "1"}]},
        "utf8": {"outcome": "exact", "columns": [{"type": "String", "expect": "a"}]},
    })[0])
    assert "BIGINT" in md and "VARCHAR" in md
    assert "`int64`" not in md and "`utf8`" not in md


def test_the_page_lists_every_column_type_that_accepts_a_key():
    md = render_types_page(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": [{"type": "Int64", "expect": "1"}, {"type": "Int128", "expect": "1"}]},
        "utf8": {"outcome": "exact", "columns": [{"type": "String", "expect": "a"}]},
    })[0])
    assert "Int64" in md and "Int128" in md


def test_the_page_separates_what_is_unsupported():
    md = render_types_page(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": [{"type": "Int64", "expect": "1"}]},
        "utf8": {"outcome": "unsupported", "code": "user.sink.type_unsupported"},
    })[0])
    assert "VARCHAR" in md.split("Unsupported")[1]
    assert "VARCHAR" not in md.split("Unsupported")[0]


def test_the_page_states_a_coercion_rule():
    """A coercion the page does not state is a surprise the reader meets in
    production."""
    md = render_types_page(LATTICE, typed({
        "int64": {"outcome": "coerced", "columns": [{"type": "Int64", "expect": "1"}],
                  "rule": "rounded to the nearest whole number"},
        "utf8": {"outcome": "exact", "columns": [{"type": "String", "expect": "a"}]},
    })[0])
    assert "rounded to the nearest whole number" in md


def test_the_page_says_it_is_generated():
    """A hand-edited copy of a generated file drifts silently."""
    md = render_types_page(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": [{"type": "Int64", "expect": "1"}]},
        "utf8": {"outcome": "exact", "columns": [{"type": "String", "expect": "a"}]},
    })[0])
    assert "Do not edit" in md
    assert "TestIntegrationSinkClickhouse_Types" in md

def test_a_text_carrier_row_names_its_cast_alone_and_says_the_rest_reach_it():
    """The utf8 row proves each destination with its own value, so it must not
    read as a cross-product of every cast and every column."""
    lattice = [{"key": "utf8", "duckdb": ["VARCHAR", "UUID", "JSON"]}]
    integ = {"types": {"utf8": {"outcome": "exact",
                                "rule": "the text must already be the column's text form.",
                                "columns": [{"type": "String", "expect": "x"},
                                            {"type": "UUID", "value": "u", "expect": "u"}]}}}
    page = render_types_page(lattice, integ)
    assert ("| `VARCHAR` | `String`, `UUID` | the text must already be the "
            "column's text form. `UUID` and `JSON` reach the sink as `VARCHAR`. |") in page


def test_a_row_with_no_per_column_value_keeps_every_cast():
    lattice = [{"key": "binary", "duckdb": ["BLOB", "BIT"]}]
    integ = {"types": {"binary": {"outcome": "exact",
                                  "columns": [{"type": "String", "expect": "x"}]}}}
    assert "| `BLOB`, `BIT` | `String` |  |" in render_types_page(lattice, integ)
