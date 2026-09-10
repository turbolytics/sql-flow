"""Build the feature and invariant coverage matrix from test suite output.

Two axes, deliberately independent:

  Feature  what sqlflow does, e.g. sink.iceberg. Declared in
           docs/coverage/features.yml. Never encodes a test level.
  Level    where the test ran: `unit` for go test -short, `integration` for
           the Go tests that need a real service, `release` for the image
           suite. Derived from which file the result came out of, never
           declared, so it cannot drift from reality.

Keeping them separate is what lets the matrix answer the question that went
unanswered for months: which features are covered by a unit test but never
proven against a real broker or the shipped image.

A skipped test does not cover its feature. It reports as SKIP, because a skip
that reads as coverage is exactly how sink.iceberg shipped untested.

Usage:
    python -m coverage_matrix --go go.json --go-integration it.json \
        --pytest pytest.json --write
    python -m coverage_matrix --go go.json --go-integration it.json \
        --pytest pytest.json --check
    python -m coverage_matrix --page

`--write` writes the status directory, the page, and the report. `--check`
exits non-zero when a feature or invariant is missing a level it requires, or
a marker names an id the registries do not declare. `--page` renders the page
from the committed status files and reads no test report.

The modules, in dependency order. Each answers one question, and an agent
changing one loads that module and its tests rather than 1500 lines:

  registries     where the declarations live, and whether they agree
  types          the Arrow type lattice, and the page it publishes
  suites         what the suites wrote, read back
  features       the feature axis
  invariants     the invariant axis
  statusfiles    the committed artifact: one status per cell
  invariantpage  the invariant half of the page
  page           the feature half, and the whole assembled
  report         the published detail: counts and names, never committed
  cli            --write, --check and --page

This file deliberately re-exports nothing. Importing the package used to mean
importing every concern, which is the shape this split exists to undo.
"""
