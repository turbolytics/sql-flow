"""Tests for coverage_matrix.published."""

from coverage_matrix.published import anchors, validate_published
from samples import *  # noqa: F401,F403

INVS = [
    {"id": "clickhouse.a", "applies_to": "sink.clickhouse"},
    {"id": "clickhouse.b", "applies_to": "sink.clickhouse"},
    {"id": "type.null", "applies_to": "sink"},
]
FEATS = [{"id": "cli.validate"}]
BOTH = "{/* invariant: clickhouse.a */}{/* invariant: clickhouse.b */}\n"


def page(tmp_path, name, text, configs=None, files=None):
    pages = tmp_path / "pages"
    pages.mkdir(exist_ok=True)
    (pages / name).write_text(text)
    cfg = tmp_path / "configs"
    cfg.mkdir(exist_ok=True)
    for k, v in (configs or {}).items():
        (cfg / k).write_text(v)
    for k, v in (files or {}).items():
        p = tmp_path / k
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(v)
    return validate_published(INVS, FEATS, [], [], pages_dir=str(pages),
                              configs_dir=str(cfg), repo=str(tmp_path),
                              types_table=False)


def test_anchors_tolerate_spacing():
    assert anchors("x {/*  invariant:  clickhouse.a  */} y") == [
        ("invariant", "clickhouse.a")]


def test_a_page_citing_every_namespaced_invariant_passes(tmp_path):
    assert page(tmp_path, "clickhouse.mdx", BOTH) == []


def test_an_uncited_namespaced_invariant_is_a_problem(tmp_path):
    problems = page(tmp_path, "clickhouse.mdx", "{/* invariant: clickhouse.a */}\n")
    assert any("clickhouse.b" in p and "cited nowhere" in p for p in problems)


def test_an_unknown_invariant_anchor_is_a_problem(tmp_path):
    problems = page(tmp_path, "clickhouse.mdx",
                    BOTH + "{/* invariant: clickhouse.zz */}")
    assert any("clickhouse.zz" in p for p in problems)


def test_a_page_with_no_namespace_needs_no_citations(tmp_path):
    assert page(tmp_path, "motherduck.mdx", "no anchors here\n") == []


def test_a_measured_anchor_must_name_a_file(tmp_path):
    text = BOTH + "{/* measured: docs/published/clickhouse/perf.out */}"
    assert any("perf.out" in p for p in page(tmp_path, "clickhouse.mdx", text))
    assert page(tmp_path, "clickhouse.mdx", text,
                files={"docs/published/clickhouse/perf.out": "x"}) == []


def test_a_config_block_must_equal_its_file(tmp_path):
    text = BOTH + "{/* config: t.yml */}\n```yaml\na: 1\n```\n"
    assert page(tmp_path, "clickhouse.mdx", text, configs={"t.yml": "a: 1\n"}) == []
    assert page(tmp_path, "clickhouse.mdx", text, configs={"t.yml": "a: 1"}) == []
    bad = page(tmp_path, "clickhouse.mdx", text, configs={"t.yml": "a: 2\n"})
    assert any("t.yml" in p and "differs" in p for p in bad)


def test_a_config_anchor_with_no_file_is_a_problem(tmp_path):
    text = BOTH + "{/* config: gone.yml */}\n```yaml\na: 1\n```\n"
    assert any("gone.yml" in p for p in page(tmp_path, "clickhouse.mdx", text))


def test_a_feature_anchor_must_name_a_feature(tmp_path):
    assert page(tmp_path, "clickhouse.mdx", BOTH + "{/* feature: cli.validate */}") == []
    assert any("cli.nope" in p for p in
               page(tmp_path, "clickhouse.mdx", BOTH + "{/* feature: cli.nope */}"))


def test_a_generated_table_must_match_the_declaration(tmp_path):
    lattice = [{"key": "int8", "duckdb": ["TINYINT"]}]
    integs = [{"id": "sink.clickhouse", "types": {"int8": {
        "outcome": "exact", "columns": [{"type": "Int8", "expect": "1"}]}}}]
    pages = tmp_path / "pages"
    pages.mkdir()
    good = BOTH + "{/* generated: type-table */}\n| `TINYINT` | `Int8` |  |\n\n"
    (pages / "clickhouse.mdx").write_text(good)
    assert validate_published(INVS, FEATS, integs, lattice, pages_dir=str(pages),
                              configs_dir=str(tmp_path), repo=str(tmp_path)) == []
    (pages / "clickhouse.mdx").write_text(good.replace("`Int8` |", "`Int16` |"))
    problems = validate_published(INVS, FEATS, integs, lattice, pages_dir=str(pages),
                                  configs_dir=str(tmp_path), repo=str(tmp_path))
    assert any("type-table" in p for p in problems)


def test_the_committed_pages_pass():
    from coverage_matrix.registries import (load_features, load_integrations,
                                            load_invariants, load_lattice)
    assert validate_published(load_invariants(), load_features(),
                              load_integrations(), load_lattice()) == []
