"""The command line: --write, --check and --page."""

import argparse
import json
import os
import sys

from .features import build, snapshot
from .invariants import build_invariants, snapshot_invariants
from .page import write_page
from .registries import (COVERAGE_DIR, MATRIX_JSON, MATRIX_MD, REPO,
                         REPORT_MD, STATUS_DIR, TYPES_PAGE,
                         load_features, load_integrations,
                         load_invariants, load_lattice,
                         validate_registries)
from .report import render_report
from .suites import parse_go, parse_pytest
from .statusfiles import read_status, status_from_snapshot, write_status
from .types import validate_types


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--go", help="go test -short -json output")
    ap.add_argument("--go-integration",
                    help="go test -json output from the integration pass")
    ap.add_argument("--pytest", help="pytest result json from the conftest hook")
    ap.add_argument("--write", action="store_true",
                    help="write the status directory, the page, and the report")
    ap.add_argument("--check", action="store_true",
                    help="exit non-zero on a gap or an unknown marker")
    ap.add_argument("--page", action="store_true",
                    help="render the page from the committed status files; "
                         "reads no test report")
    args = ap.parse_args()

    features = load_features()
    invariants = load_invariants()
    integrations = load_integrations()
    lattice = load_lattice()

    # Before any test result is read: a registry typo would otherwise reach
    # the matrix as a missing cell.
    problems = validate_registries(invariants, integrations, features)
    problems += validate_types(lattice, integrations)
    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 2

    if args.page:
        write_page(features, invariants, integrations, read_status(STATUS_DIR),
                   lattice)
        print(f"wrote {os.path.relpath(MATRIX_MD, REPO)} "
              f"and {os.path.relpath(TYPES_PAGE, REPO)}")
        return 0

    go_results, go_covers, go_invariants = (
        parse_go(args.go) if args.go and os.path.exists(args.go) else ({}, {}, {}))
    it_results, it_covers, it_invariants = (
        parse_go(args.go_integration)
        if args.go_integration and os.path.exists(args.go_integration)
        else ({}, {}, {}))
    py_results, py_covers, py_invariants = (
        parse_pytest(args.pytest)
        if args.pytest and os.path.exists(args.pytest) else ({}, {}, {}))

    coverage, secondary, unmatched, unknown = build(
        features, go_results, py_results, go_covers, py_covers,
        it_results, it_covers)
    snap = snapshot(features, coverage, secondary, unmatched, unknown)

    built = build_invariants(
        invariants, integrations,
        {"unit": go_results, "integration": it_results, "release": py_results},
        {"unit": go_invariants, "integration": it_invariants,
         "release": py_invariants})
    snap.update(snapshot_invariants(invariants, integrations, built))

    status = status_from_snapshot(snap)
    report = render_report(snap)

    if args.write:
        written = write_status(status, STATUS_DIR)
        write_page(features, invariants, integrations, status, lattice)
        os.makedirs(COVERAGE_DIR, exist_ok=True)
        with open(MATRIX_JSON, "w") as fh:
            json.dump(snap, fh, indent=2, sort_keys=False)
            fh.write("\n")
        with open(REPORT_MD, "w") as fh:
            fh.write(report)
        print(f"wrote {len(written)} files under "
              f"{os.path.relpath(STATUS_DIR, REPO)}/, "
              f"{os.path.relpath(MATRIX_MD, REPO)}, "
              f"{os.path.relpath(TYPES_PAGE, REPO)}, "
              f"{os.path.relpath(MATRIX_JSON, REPO)} "
              f"and {os.path.relpath(REPORT_MD, REPO)}")
    else:
        print(report)

    gaps = snap["gaps"] + snap["invariant_gaps"]
    unknown_markers = snap["unknown_markers"] + snap["unknown_invariant_markers"]
    if args.check and (gaps or unknown_markers):
        if gaps:
            print(f"\n{len(gaps)} coverage gap(s):", file=sys.stderr)
            for gap in snap["gaps"]:
                print(f"  {gap['feature']}: {gap['level']} is {gap['status']}",
                      file=sys.stderr)
            for gap in snap["invariant_gaps"]:
                print(f"  {gap['invariant']} on {gap['integration']}: "
                      f"{gap['level']} is {gap['status']}", file=sys.stderr)
        if unknown_markers:
            # A marker naming nothing the registries declare is a typo, and
            # the list of them no longer sits on a page anyone diffs.
            print(f"\n{len(unknown_markers)} marker(s) naming an unknown id:",
                  file=sys.stderr)
            for entry in snap["unknown_markers"]:
                print(f"  {entry['test']} marks {entry['feature']}, "
                      "which features.yml does not declare", file=sys.stderr)
            for entry in snap["unknown_invariant_markers"]:
                print(f"  {entry['test']} marks {entry['invariant']} on "
                      f"{entry['integration']}, which the registries do not "
                      "declare together", file=sys.stderr)
        return 1
    return 0
