#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["jsonschema", "pyyaml"]
# ///
"""Check render.yaml against Render's published Blueprint schema.

The schema is fetched, not vendored. Render renames plans and deprecates keys
between releases -- `autoDeploy` became `autoDeployTrigger`, and the Postgres
plan `basic-256mb` became `0.1c-256mb` -- and a vendored copy would keep
passing a file Render had already started to reject. Fetching it means CI is
what notices, rather than a failed sync against the live database.

Usage:
    bin/validate-render.py [render.yaml]

Set RENDER_SCHEMA to a local path or another URL to check against that
instead, which is also how this runs without a network.

Exit status: 0 valid, 1 invalid, 2 the schema could not be read. A caller
checking that a bad file is rejected has to require 1, because an unreachable
schema fails everything it is handed.
"""

import json
import os
import sys
import time
import urllib.request

import yaml
from jsonschema import Draft202012Validator

SCHEMA_URL = "https://render.com/schema/render.yaml.json"

# Identifies the caller in Render's logs, rather than leaving it Python-urllib.
USER_AGENT = "sql-flow render.yaml validator"

# The schema is served chunked, and a proxy between here and Render can hand
# back a short body. That reads as corrupt JSON, not as a network error, so it
# is worth several tries before believing it.
FETCH_ATTEMPTS = 5


def load_schema(source):
    """Read the schema from a URL, or from a path when given one."""
    if not source.startswith(("http://", "https://")):
        with open(source) as f:
            return json.load(f)

    request = urllib.request.Request(source, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                # Read to completion here, so a body that arrives short is a
                # retry rather than a parse error.
                return json.loads(response.read())
        except Exception as err:
            if attempt == FETCH_ATTEMPTS:
                raise
            print(
                f"fetching {source} failed ({type(err).__name__}), "
                f"retrying {attempt}/{FETCH_ATTEMPTS - 1}",
                file=sys.stderr,
            )
            time.sleep(attempt)


def leaves(error):
    """Flatten an error to the ones that name a field.

    A service is a union of per-type schemas, so a bad `plan` surfaces as the
    whole service dict being "not valid under any of the given schemas", with
    every branch's complaints underneath. Reporting the wrong branch is worse
    than reporting nothing: the union's `keyValue` branch wants an
    `ipAllowList` this file will never have.

    Services are discriminated by `type`, so the branch that did not object to
    `type` is the one the author meant. Among the rest, the branch that
    objected least.
    """
    if not error.context:
        return [error]

    branches = {}
    for sub in error.context:
        branches.setdefault(sub.schema_path[0], []).append(sub)

    matched = [
        errors
        for errors in branches.values()
        if not any(list(e.path)[-1:] == ["type"] for e in errors)
    ]
    chosen = min(matched or list(branches.values()), key=len)

    return [leaf for sub in chosen for leaf in leaves(sub)]


def report(errors):
    """Format errors as `path: message`, dropping cascade noise."""
    # `unevaluatedProperties: false` at the root re-reports every top-level key
    # whenever anything beneath it fails. Only useful when it is the sole error.
    if len(errors) > 1:
        errors = [e for e in errors if e.validator != "unevaluatedProperties" or e.path]

    lines = []
    for error in errors:
        for leaf in leaves(error):
            where = ".".join(str(p) for p in leaf.absolute_path) or "(root)"
            line = f"{where}: {leaf.message}"
            if line not in lines:
                lines.append(line)
    return lines


def main(argv):
    path = argv[1] if len(argv) > 1 else "render.yaml"
    source = os.environ.get("RENDER_SCHEMA", SCHEMA_URL)

    try:
        schema = load_schema(source)
    except Exception as err:
        print(f"could not read {source}: {type(err).__name__}: {err}", file=sys.stderr)
        return 2

    with open(path) as f:
        document = yaml.safe_load(f)

    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(document), key=lambda e: list(e.path))
    if errors:
        for line in report(errors):
            print(f"{path}: {line}", file=sys.stderr)
        print(f"{path}: invalid against {source}", file=sys.stderr)
        return 1

    print(f"{path}: valid against {source}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
