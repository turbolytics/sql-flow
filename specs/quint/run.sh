#!/usr/bin/env bash
# Runs the Quint models. Needs node; downloads the Rust evaluator on first run.
set -euo pipefail
cd "$(dirname "$0")"
Q=(npx --yes @informalsystems/quint@latest)
echo "== typecheck =="
"${Q[@]}" typecheck window_watermark.qnt
echo "== #417 current design: expect a counterexample =="
"${Q[@]}" run window_watermark.qnt --invariant=noOpenBucketRefused --max-samples=20000 || true
echo "== #417 per-partition fix: expect no violation =="
sed 's/FIX = false/FIX = true/' window_watermark.qnt > /tmp/window_watermark_fixed.qnt
"${Q[@]}" run /tmp/window_watermark_fixed.qnt --invariant=noOpenBucketRefused --max-samples=50000
