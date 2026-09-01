#!/usr/bin/env bash
set -euo pipefail

# Downloads the pinned libduckdb shared library for linux. sqlflow never links
# against DuckDB: the ADBC driver manager dlopens the library at runtime, so
# every place that runs sqlflow on linux -- the container image, the in-network
# benchmark and CI -- needs its own copy. All of them call this script so the
# version lives in exactly one place, the DUCKDB_VERSION file.
#
# Usage: install-libduckdb.sh [dest-dir] [filename]

DEST_DIR="${1:-bin}"
FILENAME="${2:-libduckdb.so}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB_VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/DUCKDB_VERSION")"

case "$(uname -m)" in
    x86_64)        DUCKDB_ARCH="amd64" ;;
    aarch64|arm64) DUCKDB_ARCH="arm64" ;;
    *)
        echo "install-libduckdb: unsupported architecture $(uname -m)" >&2
        exit 1
        ;;
esac

DEST="$DEST_DIR/$FILENAME"
if [ -f "$DEST" ]; then
    echo "libduckdb $DUCKDB_VERSION already present at $DEST"
    exit 0
fi

for tool in curl unzip; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "install-libduckdb: $tool is required but not installed" >&2
        exit 1
    fi
done

echo "--- Downloading libduckdb $DUCKDB_VERSION (linux-$DUCKDB_ARCH) -> $DEST ---"
mkdir -p "$DEST_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

curl -fL -o "$TMP_DIR/libduckdb.zip" \
    "https://github.com/duckdb/duckdb/releases/download/$DUCKDB_VERSION/libduckdb-linux-$DUCKDB_ARCH.zip"
unzip -o -j -d "$TMP_DIR" "$TMP_DIR/libduckdb.zip" libduckdb.so
mv "$TMP_DIR/libduckdb.so" "$DEST"

echo "Installed $DEST"
