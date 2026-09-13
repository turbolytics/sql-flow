"""Where the declarations live, and whether they agree.

Four registries and the paths of everything generated from them.
validate_registries runs before any test result is read: a typo would
otherwise reach the matrix as a missing cell, and "missing" is the signal
the matrix exists to carry.
"""

import os

import yaml


# scripts/coverage_matrix/registries.py -> the repository root. Located from
# this file rather than the working directory, because the generator runs from
# the Makefile, from CI, and from pytest, each with its own cwd.
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


REGISTRY = os.path.join(REPO, "docs", "coverage", "features.yml")


INVARIANTS = os.path.join(REPO, "docs", "coverage", "invariants.yml")


# One file per integration, named for its id, beside its status file of the
# same name. The single file was 602 lines and ClickHouse's type table was 369
# of them, so an agent adding a webhook exemption read all of it to find the
# 25 lines it needed.
INTEGRATIONS_DIR = os.path.join(REPO, "docs", "coverage", "integrations")


LATTICE = os.path.join(REPO, "docs", "coverage", "lattice.yml")


# What is committed: one status per (feature, level) and per (invariant,
# integration, level), one file per integration. Nothing with a test name in
# it. A test added inside a covered feature changes no committed file, so it
# cannot go stale and cannot conflict.
STATUS_DIR = os.path.join(REPO, "docs", "coverage", "status")


# The page is rendered from the status files and the registries alone, so
# anyone regenerates it without a test report.
MATRIX_MD = os.path.join(REPO, "docs", "coverage", "matrix.md")


# The full snapshot and the report carry test names and counts. They are
# written here, which is gitignored, and CI publishes them on every run.
COVERAGE_DIR = os.path.join(REPO, ".coverage")


MATRIX_JSON = os.path.join(COVERAGE_DIR, "matrix.json")


REPORT_MD = os.path.join(COVERAGE_DIR, "report.md")


# The fragment the ClickHouse integration page publishes. Generated here so it
# cannot drift from the declaration the type runner judges.
TYPES_PAGE = os.path.join(REPO, "docs", "coverage", "clickhouse-types.mdx")


PASS, SKIP, FAIL = "pass", "skip", "fail"


# Ordered cheapest to most real, which is the order they run in and the order
# the matrix reads left to right.
LEVELS = ("unit", "integration", "release")


# Go's integration pass selects its tests by name, because `go test -run` is
# the only selector that needs no build tag and no second package.
INTEGRATION_PREFIX = "TestIntegration"


# The closed vocabularies invariants.yml may use. A value outside one of them
# is a typo, and a typo must not reach the matrix as a missing cell.
FAMILIES = ("resilience", "checkpoint", "types", "lifecycle", "errors")


# What kind of claim an invariant makes, which is orthogonal to its family.
#
#   safety    nothing bad happens: never commit a position for rows the sink
#             did not take, never lose a batch a flush could not deliver.
#   liveness  something good eventually happens: buffered rows reach the sink,
#             a drain finishes, a stalled configuration fails at startup.
#
# The distinction is not academic. A sink that never flushes satisfies every
# safety invariant here -- keeps_batch holds vacuously if you never flush, and
# commit.after_flush holds if you never commit. Only a liveness claim says the
# pipeline does anything at all.
CLASSES = ("safety", "liveness")


KINDS = ("sink", "source", "handler", "pipeline", "manager")


# How an invariant collects evidence. Both are explicit markers: a test says
# what it proves. `named` used to mean "a test whose name matches the id",
# which was declared, never implemented, and would have been a third way to
# attribute the same claim.
VERIFIERS = ("harness", "typetable")


# What a type table may say a sink does with an Arrow type. Anything else is a
# typo, and a typo must not reach the matrix as a missing cell.
OUTCOMES = ("exact", "coerced", "unsupported")


PIPELINE = "pipeline"


# A pipeline configuration is an integration of the harness, though no
# constructor switch builds one. `constructed: false` says so, and the Kinds()
# agreement tests skip those entries. A manager is the same: buildManagedTables
# builds the one kind there is, and no switch lists it.
INTEGRATION_KINDS = ("sink", "source", "handler", "pipeline", "manager")


def load_features():
    with open(REGISTRY) as fh:
        return yaml.safe_load(fh)["features"]


def load_invariants():
    with open(INVARIANTS) as fh:
        return yaml.safe_load(fh)["invariants"]


def load_integrations(directory=None):
    """Every integration the directory declares, in filename order.

    Filename order is id order, because the filename is the id. That is what
    orders the columns on the page, and it is stable without anything having
    to declare it.

    Each entry carries the file it came from. validate_registries holds the
    name and the id equal, and it is handed entries rather than a directory,
    so the entry has to carry its origin.
    """
    out = []
    for name in sorted(os.listdir(directory or INTEGRATIONS_DIR)):
        if not name.endswith(".yml"):
            continue
        with open(os.path.join(directory or INTEGRATIONS_DIR, name)) as fh:
            entry = yaml.safe_load(fh)
        entry["source_file"] = name
        out.append(entry)
    return out


def load_lattice():
    with open(LATTICE) as fh:
        return yaml.safe_load(fh)["lattice"]


def validate_registries(invariants, integrations, features):
    """Every problem a human can fix, one line each. Empty when consistent.

    Checked before any test result is read. A registry typo would otherwise
    reach the matrix as a missing cell, and "missing" is the signal the matrix
    exists to carry: it must not be spendable on typos.
    """
    problems = []

    by_id = {}
    for inv in invariants:
        fid = inv.get("id")
        if fid in by_id:
            problems.append(f"invariants.yml: duplicate id {fid}")
        by_id[fid] = inv

        for key in ("family", "class", "applies_to", "claim", "verified_by",
                    "requires"):
            if key not in inv:
                problems.append(f"invariants.yml: {fid} has no {key}")
        if inv.get("family") not in FAMILIES:
            problems.append(
                f"invariants.yml: {fid} family {inv.get('family')!r} is not one of {FAMILIES}")
        if inv.get("class") not in CLASSES:
            problems.append(
                f"invariants.yml: {fid} class {inv.get('class')!r} is not one of {CLASSES}")
        if inv.get("applies_to") not in KINDS:
            problems.append(
                f"invariants.yml: {fid} applies_to {inv.get('applies_to')!r} is not one of {KINDS}")
        if inv.get("verified_by") not in VERIFIERS:
            problems.append(
                f"invariants.yml: {fid} verified_by {inv.get('verified_by')!r} is not one of {VERIFIERS}")
        for lvl in inv.get("requires", []):
            if lvl not in LEVELS:
                problems.append(
                    f"invariants.yml: {fid} requires {lvl!r}, which is not a level")

    feature_ids = {f["id"] for f in features}
    seen = set()
    for integ in integrations:
        iid = integ.get("id")
        if iid in seen:
            problems.append(f"integrations/: duplicate id {iid}")
        seen.add(iid)

        # The filename is the id, so a file that says otherwise is a rename
        # half done: the entry would go looking for a status file of the
        # other name. Skipped for entries built in a test, which have no file.
        source = integ.get("source_file")
        if source and source != f"{iid}.yml":
            problems.append(
                f"integrations/{source}: declares id {iid}, so it belongs in "
                f"{iid}.yml")

        if integ.get("kind") not in INTEGRATION_KINDS:
            problems.append(
                f"integrations/{iid}.yml: kind {integ.get('kind')!r} is not one of {INTEGRATION_KINDS}")
        # A test-only integration ships to nobody, so it has no feature and no
        # cells. It exists so the conformance harness's doubles have an id
        # that is not a real sink's.
        if not integ.get("test_only") and integ.get("feature") not in feature_ids:
            problems.append(
                f"integrations/{iid}.yml: names feature {integ.get('feature')!r}, "
                "which features.yml does not declare")

        for ex in integ.get("exempt", []):
            inv = by_id.get(ex.get("invariant"))
            if inv is None:
                problems.append(
                    f"integrations/{iid}.yml: is exempt from {ex.get('invariant')!r}, "
                    "which invariants.yml does not declare")
                continue
            if not ex.get("reason"):
                problems.append(
                    f"integrations/{iid}.yml: exemption from {inv['id']} has no reason")
            # An exemption left as prose is an excuse. Two of them hid live
            # batch-loss bugs: sink.sqlcommand and sink.console were both
            # excused because nothing crosses a network, which is the argument
            # for skipping a retry ladder, not the invariant a ladder depends
            # on. A test must prove the premise.
            if not ex.get("proven_by"):
                problems.append(
                    f"integrations/{iid}.yml: exemption from {inv['id']} has no "
                    "proven_by naming a test that proves the premise")
            if inv.get("applies_to") != integ.get("kind"):
                problems.append(
                    f"integrations/{iid}.yml: is a {integ.get('kind')} but "
                    f"{inv['id']} applies_to {inv.get('applies_to')}")

    return problems
