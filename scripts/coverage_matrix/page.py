"""The committed page: its feature half, and the whole assembled.

Rendered from the status files and the registries and nothing else. That is
what lets anyone regenerate it without Docker, lets a conflict on it be
resolved by rerunning the renderer, and lets the Tooling job check it in
seconds before any suite runs.
"""

from .features import feature_gaps
from .invariants import (by_kind, declare, exemptions_of, invariant_gaps,
                         invariant_tally)
from .invariantpage import render_invariants
from .registries import LEVELS, MATRIX_MD, TYPES_PAGE
from .types import render_types_page


def page_snapshot(features, invariants, integrations, status):
    """The page's input: the same entries the full snapshot carries, built
    from the status files and the registries instead of from test reports.

    A row the status directory lacks reads as missing where the registry
    requires it: a feature added before the next generation renders honestly
    rather than crashing the page, and the gate's staleness check is what
    catches the directory being behind.
    """
    out = {"features": [], "gaps": [], "invariants": [], "invariant_gaps": []}

    for feature in features:
        required = set(feature.get("requires", []))
        rows = status["features"].get(feature["id"], {})
        levels = {}
        for lvl in LEVELS:
            fallback = "missing" if lvl in required else "not_required"
            levels[lvl] = {"status": rows.get(lvl, fallback)}
        out["features"].append({
            "id": feature["id"],
            "description": feature["description"],
            "requires": sorted(required),
            "levels": levels,
        })
    out["gaps"] = feature_gaps(out["features"])

    kinds = by_kind(integrations)
    exemptions = exemptions_of(integrations)
    for inv in invariants:
        entry = declare(inv)
        for integ in kinds.get(inv["applies_to"], []):
            # The registry is the declaration. A status row that disagrees
            # with an exemption is stale, and the gate reports that; the
            # page does not repeat the disagreement.
            if (inv["id"], integ["id"]) in exemptions:
                levels = {lvl: {"status": "exempt"} for lvl in LEVELS}
            else:
                rows = status["integrations"].get(integ["id"], {}).get(inv["id"], {})
                levels = {lvl: {"status": rows.get(lvl, "missing")} for lvl in LEVELS}
            entry["integrations"][integ["id"]] = levels
        out["invariants"].append(entry)
    out["invariant_gaps"] = invariant_gaps(out["invariants"])

    return out


def render_page(features, invariants, integrations, status):
    """matrix.md, from committed files alone."""
    ps = page_snapshot(features, invariants, integrations, status)
    return render(ps) + "\n" + render_invariants(ps)


MARK = {
    "covered": "✅",
    "skipped": "⚠️ skipped",
    "missing": "❌ **missing**",
    "failing": "🔥 failing",
    "not_required": "—",
}


def render(snap):
    """Render the human view. Everything here comes from the snapshot, so the
    markdown can never disagree with the JSON that gates the build."""
    lines = [
        "# Feature coverage matrix",
        "",
        "Generated from `docs/coverage/status/` by `make coverage-page`.",
        "Do not edit by hand.",
        "",
        "Features are declared in `docs/coverage/features.yml`. A test attaches",
        "to one by saying so: `coverage.Covers(t, \"sink.clickhouse\")` in Go, or",
        "the `covers` marker in pytest. A test used to attach by the shape of",
        "its name, which credited a test whose name merely started the same way",
        "and credited nothing when a name drifted.",
        "",
        "Levels are derived from where a test ran, never declared, so they",
        "cannot drift. `unit` is `go test -short`, `integration` is the Go",
        "tests that need a real service, `release` is the image suite. A",
        "**skipped** test is not coverage at any of them: a skip that reads as",
        "a pass is how `sink.iceberg` shipped for months without ever being",
        "written to.",
        "",
        "Only statuses are committed. Test names and counts are in the coverage",
        "report CI publishes on every run: they change whenever a test is",
        "added, and this page changes only when a status does.",
        "",
        "| Feature | What it does | " + " | ".join(LEVELS) + " |",
        "| --- | --- | " + " | ".join("---" for _ in LEVELS) + " |",
    ]

    for feature in snap["features"]:
        cells = " | ".join(MARK[feature["levels"][lvl]["status"]]
                           for lvl in LEVELS)
        lines.append(
            f"| `{feature['id']}` | {feature['description']} | {cells} |")

    covered = sum(
        1 for f in snap["features"]
        if all(f["levels"][lvl]["status"] in ("covered", "not_required")
               for lvl in LEVELS)
    )
    lines += [
        "",
        f"**{len(snap['features'])} features declared. {covered} have at least one "
        f"passing test attributed at every level they require, so "
        f"{len(snap['gaps'])} gap(s).**",
        "",
        "That sentence counts attribution, not proof. A feature is green here when",
        "a test named for it ran and passed; it says nothing about whether the",
        "integration behind it keeps a batch it could not deliver, or commits",
        "offsets only after a flush. Those are invariants, they are counted",
        "separately below, and the two numbers are not interchangeable.",
        "",
    ]

    if snap.get("invariants"):
        tally, unwired = invariant_tally(snap)
        total = sum(tally.values())
        safety = sum(1 for i in snap["invariants"] if i["class"] == "safety")
        liveness = sum(1 for i in snap["invariants"] if i["class"] == "liveness")

        lines += [
            f"**{len(snap['invariants'])} invariants declared: {safety} safety "
            f"and {liveness} liveness. Of {total} (invariant, integration) "
            f"cells: {tally['covered']} proven, {tally['missing']} missing, "
            f"{tally['skipped']} skipped, {tally['failing']} failing, "
            f"{tally['exempt']} exempt. {len(snap['invariant_gaps'])} gap(s).**",
            "",
            "Safety says nothing bad happens. Liveness says something good",
            "eventually does, and the two are not interchangeable: a sink that",
            "never flushes satisfies every safety invariant on this page.",
            "`keeps_batch` holds if you never flush, and `commit.after_flush`",
            "holds if you never commit. Only a liveness claim says the pipeline",
            "does anything at all.",
            "",
        ]
        if unwired:
            lines += [
                f"A further {unwired} invariants carry no cell at all, so nothing",
                "can prove them. That is a declaration with no path to evidence,",
                "and it is worse than missing rather than better.",
                "",
            ]

    if snap["gaps"]:
        lines += ["## Gaps", "",
                  "These fail `make coverage-matrix`. There is no baseline: a gap",
                  "is closed by a test, or by the registry honestly no longer",
                  "requiring that level.", ""]
        for gap in snap["gaps"]:
            lines.append(
                f"- `{gap['feature']}` requires **{gap['level']}** coverage "
                f"and is *{gap['status']}*.")
        lines.append("")

    return "\n".join(lines) + "\n"


def write_page(features, invariants, integrations, status, lattice):
    """matrix.md and the types page, from committed files alone."""
    with open(MATRIX_MD, "w") as fh:
        fh.write(render_page(features, invariants, integrations, status))
    # The published page is generated from the registries alone, not from any
    # test report: it says what the declaration claims, and the type runner is
    # what holds the declaration to the sink.
    clickhouse = next(
        (i for i in integrations if i["id"] == "sink.clickhouse"), None)
    if clickhouse:
        with open(TYPES_PAGE, "w") as fh:
            fh.write(render_types_page(lattice, clickhouse))
