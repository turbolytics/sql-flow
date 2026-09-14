#!/usr/bin/env bash
# Run component leak loops on Linux, each test in its own process.
#
#   dev/bench/leakloops.sh <out-dir> <test regexp> <package>...
#
#   dev/bench/leakloops.sh leak-out 'TestSinkSQLCommand__Postgres' ./internal/sinks
#
# The loops build only with -tags leakloop: they report a rate and assert
# nothing, so they are not part of the test suite. One process per test
# because a loop inherits the allocator state of every loop that ran before it
# in the same binary: the third StructuredBatch loop started 78 MiB above the
# first. Writes <out-dir>/<Test>.txt.
#
# Environment, passed to every loop:
#   SQLFLOW_LEAK_SCALE        multiplies every loop's event count (default 1)
#   SQLFLOW_LEAK_JETSTREAM    host path to a capture from record.py
#   SQLFLOW_LEAK_POSTGRES     connection string as the container sees it
#   SQLFLOW_LEAK_MALLOC_TRIM  malloc_trim(0) before every sample when set
#   MALLOC_ARENA_MAX          glibc arena limit
#   BENCH_NETWORK             docker network (default dev_default)
set -euo pipefail

out=${1:?out-dir}; pattern=${2:?test regexp}; shift 2
[ $# -gt 0 ] || { echo "name at least one package" >&2; exit 2; }
root=$(cd "$(dirname "$0")/../.." && pwd)
image=sqlflow-leakloop
mkdir -p "$out"; out=$(cd "$out" && pwd)

if ! docker image inspect "$image" >/dev/null 2>&1; then
  docker build -q -f "$root/dev/bench/leakloop.Dockerfile" -t "$image" "$root" >/dev/null
fi

args=(--rm --network "${BENCH_NETWORK:-dev_default}"
  -v "$root":/src -v "$out":/out
  -v sqlflow-leakloop-gomod:/go/pkg/mod -v sqlflow-leakloop-gocache:/root/.cache/go-build)
for v in SQLFLOW_LEAK_SCALE SQLFLOW_LEAK_POSTGRES SQLFLOW_LEAK_MALLOC_TRIM MALLOC_ARENA_MAX; do
  [ -n "${!v:-}" ] && args+=(-e "$v=${!v}")
done
if [ -n "${SQLFLOW_LEAK_JETSTREAM:-}" ]; then
  args+=(-v "$(cd "$(dirname "$SQLFLOW_LEAK_JETSTREAM")" && pwd)/$(basename "$SQLFLOW_LEAK_JETSTREAM")":/capture:ro
    -e SQLFLOW_LEAK_JETSTREAM=/capture)
fi

docker run "${args[@]}" "$image" bash -c '
  set -uo pipefail
  pattern=$1; shift
  cd /src
  for pkg in "$@"; do
    name=$(basename "$pkg")
    go test -tags leakloop -c -o "/tmp/$name.test" "$pkg" || exit 1
    for t in $(cd "/src/$pkg" && "/tmp/$name.test" -test.list "$pattern"); do
      (cd "/src/$pkg" && "/tmp/$name.test" -test.run "^$t\$" -test.v -test.count=1 -test.timeout 120m) > "/out/$t.txt" 2>&1
      printf "%-60s %s\n" "$t" "$(grep -E "^\s+per [a-z]+ after warm-up" "/out/$t.txt" | sed "s/^ *//" || grep -E "^--- " "/out/$t.txt")"
    done
  done
' _ "$pattern" "$@"
