"""Emit a machine-readable result file for the coverage matrix.

pytest-json-report would do this, but it is one more pinned dependency for
thirty lines of hook. Set SQLFLOW_PYTEST_JSON to a path and the suite writes
its outcomes there; leave it unset and this does nothing.

The matrix needs outcomes rather than a pass/fail exit code, because a skipped
test is not coverage. Reading skips as passes is how `sink.iceberg` shipped
untested.
"""

import json
import os

_results = {}
_covers = {}


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "covers(*features): additional features this test proves, beyond the "
        "one its name claims. Use it only when a test really does cover "
        "several; a feature that needs a marker to be covered at all wants "
        "its own test.",
    )


def pytest_collection_modifyitems(items):
    for item in items:
        extra = []
        for marker in item.iter_markers(name="covers"):
            extra.extend(marker.args)
        if extra:
            _covers[item.nodeid.rsplit("::", 1)[-1]] = sorted(set(extra))


def pytest_runtest_logreport(report):
    # A test has setup, call and teardown phases. The call phase is the test
    # itself; a skip raised in setup never reaches it, so both are recorded and
    # the worse outcome wins.
    if report.when not in ("setup", "call"):
        return

    name = report.nodeid.rsplit("::", 1)[-1]
    if report.failed:
        outcome = "failed"
    elif report.skipped:
        outcome = "skipped"
    elif report.when == "call" and report.passed:
        outcome = "passed"
    else:
        return

    # failed beats skipped beats passed, so a test that skipped in setup does
    # not report as passed on a later phase.
    rank = {"passed": 0, "skipped": 1, "failed": 2}
    if name not in _results or rank[outcome] > rank[_results[name]]:
        _results[name] = outcome


def pytest_sessionfinish(session, exitstatus):
    path = os.environ.get("SQLFLOW_PYTEST_JSON")
    if not path:
        return
    with open(path, "w") as fh:
        json.dump(
            {"tests": [{"nodeid": name,
                        "outcome": outcome,
                        "covers": _covers.get(name, [])}
                       for name, outcome in sorted(_results.items())]},
            fh,
            indent=2,
        )
