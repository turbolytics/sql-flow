"""Published pages and the claims they make.

A page under docs/published carries an anchor comment beside each claim:
{/* invariant: id */}, {/* measured: path */}, {/* config: file */},
{/* feature: id */} or {/* generated: type-table */}. This holds the page to
the registries. Every anchor resolves, every invariant in the page's
namespace is cited, and every embedded config and generated table matches
its source. A page that drifts from its evidence fails here, rather than in a
reader's pipeline.
"""

import os
import re

from .registries import PUBLISHED_CONFIGS, PUBLISHED_DIR, REPO
from .types import render_types_page

ANCHOR = re.compile(
    r"\{/\*\s*(invariant|measured|config|feature|generated)\s*:\s*(.+?)\s*\*/\}")
FENCE = re.compile(r"```[a-z]*\n(.*?)```", re.S)

# What each generated anchor compares, and the line prefix that marks its rows.
GENERATED = {
    "type-table": "| `",
    "unsupported-types": "- ",
}


def anchors(text):
    """Every anchor in the page, as (kind, value), in page order."""
    return [(m.group(1), m.group(2)) for m in ANCHOR.finditer(text)]


def _block_after(text, end):
    """The first fenced block after offset end, without its fences."""
    m = FENCE.search(text, end)
    return m.group(1) if m else None


def _rows_after(text, end, prefix):
    """The run of lines starting with prefix that first follows offset end."""
    rows = []
    for line in text[end:].splitlines():
        if line.startswith(prefix):
            rows.append(line)
        elif rows:
            break
    return rows


def _generated(lattice, integrations):
    """The rows render_types_page emits for sink.clickhouse, by anchor."""
    integ = next((i for i in integrations if i["id"] == "sink.clickhouse"), None)
    if integ is None:
        return {name: [] for name in GENERATED}
    lines = render_types_page(lattice, integ).splitlines()
    return {name: [l for l in lines if l.startswith(prefix)]
            for name, prefix in GENERATED.items()}


def validate_published(invariants, features, integrations, lattice,
                       pages_dir=PUBLISHED_DIR, configs_dir=PUBLISHED_CONFIGS,
                       repo=REPO, types_table=True):
    """Every disagreement between a published page and its evidence, one line
    each. Empty when every page holds."""
    problems = []
    if not os.path.isdir(pages_dir):
        return problems
    inv_ids = {i["id"] for i in invariants}
    feat_ids = {f["id"] for f in features}
    generated = _generated(lattice, integrations) if types_table else {}

    for name in sorted(os.listdir(pages_dir)):
        if not name.endswith(".mdx"):
            continue
        path = os.path.join(pages_dir, name)
        rel = os.path.relpath(path, repo)
        with open(path) as fh:
            text = fh.read()

        cited = set()
        for m in ANCHOR.finditer(text):
            kind, value = m.group(1), m.group(2)
            if kind == "invariant":
                cited.add(value)
                if value not in inv_ids:
                    problems.append(f"{rel}: cites invariant {value}, which "
                                    "invariants.yml does not declare")
            elif kind == "feature":
                if value not in feat_ids:
                    problems.append(f"{rel}: cites feature {value}, which "
                                    "features.yml does not declare")
            elif kind == "measured":
                if not os.path.isfile(os.path.join(repo, value)):
                    problems.append(f"{rel}: cites measurement {value}, which "
                                    "does not exist")
            elif kind == "config":
                source = os.path.join(configs_dir, value)
                if not os.path.isfile(source):
                    problems.append(f"{rel}: embeds config {value}, which does "
                                    "not exist")
                    continue
                with open(source) as fh:
                    want = fh.read()
                block = _block_after(text, m.end())
                # Only the final newline may differ: the fence supplies one.
                if block is None or block.rstrip("\n") != want.rstrip("\n"):
                    problems.append(f"{rel}: the block after config {value} "
                                    "differs from the published file")
            elif kind == "generated" and types_table:
                if value not in GENERATED:
                    problems.append(f"{rel}: generated {value} is not one of "
                                    f"{sorted(GENERATED)}")
                elif _rows_after(text, m.end(), GENERATED[value]) != generated[value]:
                    problems.append(f"{rel}: the {value} differs from the one "
                                    "the type declaration generates")

        # A page named for a namespace answers for every invariant in it. A
        # page with no namespace of its own, such as a tutorial, owes none.
        namespace = name[:-len(".mdx")] + "."
        for iid in sorted(inv_ids):
            if iid.startswith(namespace) and iid not in cited:
                problems.append(f"{rel}: invariant {iid} is cited nowhere on "
                                "the page")
    return problems
