#!/usr/bin/env bash
# Checks the Quint models. `run.sh` samples (Node only); `run.sh verify` also
# runs the exhaustive Apalache backend, which needs a JVM on PATH.
set -euo pipefail
cd "$(dirname "$0")"
Q=(npx --yes @informalsystems/quint@latest)
VERIFY="${1:-}"

check() { # file invariant maxsteps
  local f=$1 inv=$2 steps=$3
  echo "== $f :: $inv =="
  "${Q[@]}" typecheck "$f"
  echo "-- current design (FIX=false): expect a counterexample"
  "${Q[@]}" run "$f" --invariant="$inv" --max-samples=20000 || true
  local fixed="/tmp/$(basename "$f" .qnt)_fixed.qnt"
  sed 's/FIX = false/FIX = true/' "$f" > "$fixed"
  echo "-- fix (FIX=true): expect no violation"
  "${Q[@]}" run "$fixed" --invariant="$inv" --max-samples=50000
  if [ "$VERIFY" = "verify" ]; then
    echo "-- verify current design: expect a counterexample"
    "${Q[@]}" verify "$f" --invariant="$inv" --max-steps="$steps" || true
    echo "-- verify fix: expect NoError"
    "${Q[@]}" verify "$fixed" --invariant="$inv" --max-steps="$steps"
  fi
}

check window_watermark.qnt noOpenBucketRefused    12
check single_writer.qnt    noNonOwnerPublish      10
check commit_offsets.qnt   commitsOnlyOwned       10
check commit_offsets.qnt   committedBelowRetained 10
check exactly_once.qnt     exactlyOnce            10
