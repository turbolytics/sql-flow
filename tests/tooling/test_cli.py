"""Tests for coverage_matrix.cli.

The gate as CI runs it: a subprocess, through the same entry point the
Makefile uses. Calling main() in-process would prove the functions agree and
say nothing about whether `python -m coverage_matrix` resolves.
"""

import json
import os
import subprocess
import sys

from coverage_matrix.registries import REPO
from samples import *  # noqa: F401,F403


def run_check(go):
    """The generator, invoked the way the Makefile invokes it."""
    env = dict(os.environ, PYTHONPATH=os.path.join(REPO, "scripts"))
    return subprocess.run(
        [sys.executable, "-m", "coverage_matrix", "--go", go, "--check"],
        capture_output=True, text=True, env=env, cwd=REPO)


def test_the_module_runs_as_a_command():
    """The Makefile runs `python -m coverage_matrix`. A package whose
    __main__ does not resolve fails every coverage target at once."""
    env = dict(os.environ, PYTHONPATH=os.path.join(REPO, "scripts"))
    proc = subprocess.run(
        [sys.executable, "-m", "coverage_matrix", "--help"],
        capture_output=True, text=True, env=env, cwd=REPO)

    assert proc.returncode == 0, proc.stderr
    assert "--page" in proc.stdout


def test_the_check_exits_non_zero_on_a_gap(tmp_path):
    """The gate itself, end to end."""
    go = write(tmp_path, "go.json", json.dumps(
        {"Action": "skip", "Test": "TestSinkClickhouse_InsertsRows"}))
    proc = run_check(go)

    assert proc.returncode == 1, proc.stdout
    assert "coverage gap" in proc.stderr


def test_the_check_exits_non_zero_on_an_unknown_marker(tmp_path):
    """Once the unknown-marker list left the reviewed page, failing is the
    only way it stays visible."""
    go = write(tmp_path, "go.json", "\n".join([
        json.dumps({"Action": "output", "Test": "TestA",
                    "Output": "    x.go:1: COVERS sink.nonexistent\n"}),
        json.dumps({"Action": "pass", "Test": "TestA"}),
    ]))
    proc = run_check(go)

    assert proc.returncode == 1, proc.stdout
    assert "TestA marks sink.nonexistent" in proc.stderr
    assert "# Coverage report" in proc.stdout