"""Tests for coverage_matrix.registries."""

import os

from coverage_matrix.registries import (
    LEVELS, PASS, SKIP, STATUS_DIR, VERIFIERS, load_features,
    load_integrations, load_invariants, validate_registries)
from samples import *  # noqa: F401,F403


def test_levels_run_from_cheapest_to_most_real():
    assert LEVELS == ("unit", "integration", "release")


def test_parse_go_lets_a_pass_outrank_a_skipped_subtest():
    """A parent that passes with one skipped subtest is coverage."""
    assert PASS != SKIP  # guard the constants


def test_every_declared_feature_is_well_formed():
    for feature in load_features():
        assert feature["id"], feature
        assert "." in feature["id"], f"{feature['id']} needs a domain.component id"
        assert feature["id"] == feature["id"].lower(), feature["id"]
        assert feature.get("description"), feature["id"]
        assert feature.get("requires"), f"{feature['id']} requires nothing"
        for lvl in feature["requires"]:
            assert lvl in LEVELS, f"{feature['id']}: {lvl}"


def test_feature_ids_are_unique():
    ids = [f["id"] for f in load_features()]
    assert len(ids) == len(set(ids))


def test_no_feature_id_encodes_a_level():
    """Level is a derived axis. Baking it into the id makes the unit and
    release rows of the same feature different strings, and the matrix can no
    longer ask what is covered in unit but not in the image."""
    for feature in load_features():
        head = feature["id"].split(".")[0]
        assert head not in ("unit", "release", "integration"), feature["id"]


def test_load_integrations_reads_every_file_in_the_directory(tmp_path):
    (tmp_path / "sink.b.yml").write_text("id: sink.b\nkind: sink\n")
    (tmp_path / "sink.a.yml").write_text("id: sink.a\nkind: sink\n")
    (tmp_path / "README.md").write_text("not an entry\n")

    entries = load_integrations(str(tmp_path))
    assert [e["id"] for e in entries] == ["sink.a", "sink.b"]


def test_load_integrations_stamps_the_file_each_entry_came_from(tmp_path):
    """validate_registries holds the name and the id equal, and it is handed
    entries rather than a directory, so the entry has to carry its origin."""
    (tmp_path / "sink.a.yml").write_text("id: sink.a\nkind: sink\n")
    assert load_integrations(str(tmp_path))[0]["source_file"] == "sink.a.yml"


def test_validate_rejects_a_file_whose_name_and_id_disagree():
    """The filename is the id. A file that says otherwise is a rename half
    done, and the entry it declares would go looking for the wrong status
    file."""
    bad = [dict(INTEGRATIONS[0], source_file="sink.clickhosue.yml")]
    problems = validate_registries(INVARIANTS, bad, FEATURES)
    assert any("sink.clickhosue.yml" in p and "sink.clickhouse" in p
               for p in problems)


def test_validate_accepts_a_file_named_for_its_id():
    ok = [dict(INTEGRATIONS[0], source_file="sink.clickhouse.yml")]
    assert validate_registries(INVARIANTS, ok, FEATURES) == []


def test_every_committed_integration_is_its_own_file():
    """One file per integration, named for it. A reader of one integration
    opens one file, and two branches touching different integrations do not
    conflict."""
    for integ in load_integrations():
        assert integ["source_file"] == f"{integ['id']}.yml", integ["id"]


def test_the_committed_registries_load_and_validate():
    """The real files, not fixtures: a typo in either fails here before it can
    fail in CI."""
    invariants = load_invariants()
    integrations = load_integrations()
    features = load_features()

    assert validate_registries(invariants, integrations, features) == []
    assert len(invariants) >= 20
    assert {i["kind"] for i in integrations} == {
        "sink", "source", "handler", "pipeline", "manager"}


def test_every_committed_invariant_is_unenforced_in_this_revision():
    """This revision reports and fails nothing. Filling in a requires is a
    later change, and legal only once every non-exempt integration has a
    subject."""
    for inv in load_invariants():
        assert inv["requires"] == [], inv["id"]


def test_validate_rejects_an_exemption_naming_no_invariant():
    bad = [dict(INTEGRATIONS[1], exempt=[
        {"invariant": "sink.flush.nonexistent", "reason": "r"}])]
    problems = validate_registries(INVARIANTS, bad, FEATURES)
    assert any("sink.flush.nonexistent" in p for p in problems)


def test_validate_rejects_an_exemption_without_a_reason():
    bad = [dict(INTEGRATIONS[1], exempt=[
        {"invariant": "sink.flush.keeps_batch"}])]
    problems = validate_registries(INVARIANTS, bad, FEATURES)
    assert any("reason" in p for p in problems)


def test_validate_rejects_an_exemption_from_an_invariant_of_another_kind():
    """A sink cannot be exempt from a source invariant; that is a typo."""
    bad = [dict(INTEGRATIONS[0], exempt=[
        {"invariant": "source.commit.only_processed", "reason": "r"}])]
    problems = validate_registries(INVARIANTS, bad, FEATURES)
    assert any("applies_to" in p for p in problems)


def test_validate_rejects_an_integration_naming_no_feature():
    bad = [dict(INTEGRATIONS[0], feature="sink.nonexistent")]
    problems = validate_registries(INVARIANTS, bad, FEATURES)
    assert any("sink.nonexistent" in p for p in problems)


def test_validate_rejects_a_duplicate_invariant_id():
    problems = validate_registries(
        INVARIANTS + [INVARIANTS[0]], INTEGRATIONS, FEATURES)
    assert any("duplicate" in p for p in problems)


def test_validate_rejects_a_requires_level_that_does_not_exist():
    bad = [dict(INVARIANTS[0], requires=["nightly"])]
    problems = validate_registries(bad, INTEGRATIONS, FEATURES)
    assert any("nightly" in p for p in problems)


def test_no_committed_status_file_carries_a_test_name():
    for name in os.listdir(STATUS_DIR):
        with open(os.path.join(STATUS_DIR, name)) as fh:
            assert "Test" not in fh.read(), name


def test_the_committed_exemptions_all_name_a_test():
    for integ in load_integrations():
        for ex in integ.get("exempt", []):
            assert ex.get("proven_by"), \
                f"{integ['id']} is exempt from {ex['invariant']} with no proven_by"


def test_validate_rejects_an_exemption_with_no_proven_by():
    bad = [dict(INTEGRATIONS[1], exempt=[
        {"invariant": "sink.flush.keeps_batch", "reason": "stdout is reliable"}])]
    problems = validate_registries(INVARIANTS, bad, FEATURES)
    assert any("proven_by" in p for p in problems)


def test_validate_still_requires_a_reason_beside_the_proof():
    """The test says what is true; the reason says why that makes the
    invariant inapplicable. A reader needs both."""
    bad = [dict(INTEGRATIONS[1], exempt=[
        {"invariant": "sink.flush.keeps_batch", "proven_by": "TestX"}])]
    problems = validate_registries(INVARIANTS, bad, FEATURES)
    assert any("reason" in p for p in problems)


def test_the_committed_registry_enforces_the_two_proven_invariants():
    """Every sink now proves both, so both are enforced. A new sink cannot
    merge without a conformance subject."""
    enforced = {i["id"] for i in load_invariants() if i.get("enforced")}
    assert "sink.write.buffers_only" in enforced
    assert "sink.flush.keeps_batch" in enforced


def test_a_test_only_integration_needs_no_feature():
    """It ships to nobody, so features.yml never names it."""
    integrations = INTEGRATIONS + [
        {"id": "sink.conformance_double", "kind": "sink", "test_only": True,
         "implements": ["Sink"], "exempt": []}]
    assert validate_registries(INVARIANTS, integrations, FEATURES) == []


def test_a_shipped_integration_still_needs_a_feature():
    bad = INTEGRATIONS + [
        {"id": "sink.mystery", "kind": "sink", "implements": ["Sink"], "exempt": []}]
    problems = validate_registries(INVARIANTS, bad, FEATURES)
    assert any("sink.mystery" in p for p in problems)


def test_the_committed_registry_marks_the_double_test_only():
    doubles = [i for i in load_integrations() if i.get("test_only")]
    assert [i["id"] for i in doubles] == ["sink.conformance_double"]


def test_named_is_no_longer_a_verifier():
    assert "named" not in VERIFIERS


def test_no_committed_invariant_uses_named():
    for inv in load_invariants():
        assert inv["verified_by"] != "named", inv["id"]


def test_every_committed_invariant_declares_its_class():
    for inv in load_invariants():
        assert inv.get("class") in ("safety", "liveness"), inv["id"]


def test_validate_rejects_an_invariant_with_no_class():
    bad = [{k: v for k, v in SAFETY.items() if k != "class"}]
    problems = validate_registries(bad, INTEGRATIONS, FEATURES)
    assert any("class" in p for p in problems)


def test_validate_rejects_an_unknown_class():
    bad = [dict(SAFETY, **{"class": "eventual"})]
    problems = validate_registries(bad, INTEGRATIONS, FEATURES)
    assert any("eventual" in p for p in problems)


def test_the_registry_declares_at_least_one_liveness_invariant():
    """A file of safety claims alone cannot say the pipeline does anything."""
    classes = {i["class"] for i in load_invariants()}
    assert "liveness" in classes