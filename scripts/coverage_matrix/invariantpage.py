"""The invariant half of the page: one table per family.

A covered cell names the levels that proved it, because "proven with a
fake but never against the real thing" is the question the matrix exists
to answer, and a bare tick cannot say it.
"""

from .invariants import cell_state
from .registries import CLASSES, FAMILIES, LEVELS, PIPELINE


# Level initials, so a cell fits: u = unit, i = integration, r = release.
LEVEL_INITIALS = {lvl: lvl[0] for lvl in LEVELS}


def invariant_cell(levels, required):
    """Render one integration's cell for one invariant.

    A covered cell names the levels that proved it, because "proven with a
    fake but never against the real thing" is the question this matrix exists
    to answer, and a bare tick cannot say it.
    """
    state = cell_state(levels)
    if state == "exempt":
        return "— exempt"
    if state == "failing":
        return "🔥 failing"
    if state == "skipped":
        return "⚠️ skipped"
    if state == "missing":
        return "❌ missing"

    states = {lvl: cell["status"] for lvl, cell in levels.items()}
    proven = "".join(LEVEL_INITIALS[lvl] for lvl in LEVELS
                     if states.get(lvl) == "covered")
    # A required level that is not covered is a gap, listed below. Say so here
    # too rather than showing a tick beside an unmet requirement.
    unmet = [lvl for lvl in required if states.get(lvl) != "covered"]
    if unmet:
        return f"⚠️ {proven}, {'+'.join(unmet)} missing"
    return f"✅ {proven}"


def describe_claim(inv):
    """The claim, plus the issue tracking it when the code does not hold it."""
    claim = inv["claim"]
    if inv.get("tracked_by"):
        claim += f" *(declared, tracked by {inv['tracked_by']})*"
    if inv.get("violated_once"):
        claim += f" *(violated once: {', '.join(inv['violated_once'])})*"
    return claim


def render_invariants(snap):
    """Render the invariant half. One table per family, one column per
    integration the family's invariants apply to."""
    lines = [
        "# Invariant matrix",
        "",
        "Generated from `docs/coverage/status/` by `make coverage-page`.",
        "Do not edit by hand.",
        "",
        "Invariants are declared in `docs/coverage/invariants.yml`, integrations",
        "one per file in `docs/coverage/integrations/`. A cell is proven by the",
        "conformance harness in `internal/conformance`, which emits a marker",
        "naming both ids -- a harness test's name says nothing, because the same",
        "code runs for every integration.",
        "",
        "A covered cell names the levels that proved it: `u` unit, `i`",
        "integration, `r` release. That is the question the matrix exists to",
        "answer -- proven with a fake, or against the real thing, or in the",
        "shipped image. **exempt** carries its reason in the integration's own",
        "file, and **missing** means no evidence. Nothing here fails the build",
        "until an invariant's `requires` is filled in, and none is yet.",
        "",
    ]

    # Grouped by class first. A liveness section that is entirely red sitting
    # beside a green safety one is the imbalance a reader has to see, and a
    # column would let it pass unnoticed.
    groups = []
    for inv in snap["invariants"]:
        key = (inv["class"], inv["family"])
        if key not in groups:
            groups.append(key)
    groups.sort(key=lambda k: (CLASSES.index(k[0]), FAMILIES.index(k[1])))

    for cls, family in groups:
        rows = [i for i in snap["invariants"]
                if i["class"] == cls and i["family"] == family]

        # Split on what the invariant applies to. A pipeline row in a table of
        # sink columns renders as a line of dots, which reads as "not
        # applicable" when the truth is "unproven".
        per_integration = [i for i in rows if i["applies_to"] != PIPELINE]
        per_pipeline = [i for i in rows if i["applies_to"] == PIPELINE]

        columns = []
        for inv in per_integration:
            for integ in inv["integrations"]:
                if integ not in columns:
                    columns.append(integ)

        lines += [f"## {cls.capitalize()} invariants: {family}", ""]

        if per_integration:
            lines.append("| Invariant | Claim | "
                         + " | ".join(f"`{c}`" for c in columns) + " |")
            lines.append("| --- | --- | "
                         + " | ".join("---" for _ in columns) + " |")
            for inv in per_integration:
                cells = " | ".join(
                    invariant_cell(inv["integrations"][c], inv["requires"])
                    if c in inv["integrations"] else "·"
                    for c in columns)
                lines.append(f"| `{inv['id']}` | {describe_claim(inv)} | {cells} |")
            lines.append("")

        if per_pipeline:
            lines += [
                f"These {family} invariants are properties of the consume loop",
                "rather than of anything a config file names. The columns are",
                "its configurations, and `internal/conformance` runs each one",
                "through every path that reaches a batch: the batch filling, the",
                "flush interval elapsing, the source closing, and a cancel that",
                "drains. An invariant holds only if it holds on all four.",
                "",
            ]
            pipeline_columns = []
            for inv in per_pipeline:
                for integ in inv["integrations"]:
                    if integ not in pipeline_columns:
                        pipeline_columns.append(integ)

            lines.append("| Invariant | Claim | "
                         + " | ".join(f"`{c}`" for c in pipeline_columns) + " |")
            lines.append("| --- | --- | "
                         + " | ".join("---" for _ in pipeline_columns) + " |")
            for inv in per_pipeline:
                cells = " | ".join(
                    invariant_cell(inv["integrations"][c], inv["requires"])
                    if c in inv["integrations"] else "·"
                    for c in pipeline_columns)
                lines.append(f"| `{inv['id']}` | {describe_claim(inv)} | {cells} |")
            lines.append("")

    if snap["invariant_gaps"]:
        lines += ["## Invariant gaps", "",
                  "These fail `make coverage-check`.", ""]
        for gap in snap["invariant_gaps"]:
            lines.append(
                f"- `{gap['invariant']}` on `{gap['integration']}` requires "
                f"**{gap['level']}** and is *{gap['status']}*.")
        lines.append("")

    return "\n".join(lines) + "\n"
