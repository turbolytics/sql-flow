#!/usr/bin/env bash
# Signs a body and posts it to the pipeline.
#
#   URL=https://<ingest>.onrender.com SECRET=<secret> bin/send.sh '{"name":"hello","type":"count"}'
#
# Leave SECRET unset for a pipeline running with SQLFLOW_WEBHOOK_AUTH=none.
set -euo pipefail

body="${1:?usage: URL=... [SECRET=...] send.sh '<json>'}"
url="${URL:?URL is not set}"

args=(-sS -X POST "$url/events" -H 'Content-Type: application/json' --data-binary "$body")
if [ -n "${SECRET:-}" ]; then
  # The signature covers the exact bytes sent. printf '%s' adds no newline,
  # and --data-binary sends the body as is, where -d would strip newlines.
  sig="$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')"
  args+=(-H "X-Signature-256: sha256=$sig")
fi
curl "${args[@]}"
echo
