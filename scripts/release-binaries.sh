#!/usr/bin/env bash
set -euo pipefail

# Builds the release binaries for sqlflow.
#
# WHY THIS IS NOT JUST `GOOS=... GOARCH=... go build`
#
# sqlflow reaches DuckDB through the ADBC driver manager, which is a cgo
# package. CGO_ENABLED=0 does not merely produce a slower binary, it does not
# compile at all:
#
#     internal/duckdb/open.go:36:20: undefined: drivermgr.Driver
#
# and with CGO_ENABLED=1 the Go toolchain hands the C files to the host C
# compiler, which only targets the host OS. So every target needs a C toolchain
# for that target, and the matrix below is built three different ways.
#
#   TARGET          HOW IT IS BUILT              WHAT THE HOST NEEDS
#   linux/amd64     docker --platform            docker (+ binfmt/qemu if the
#   linux/arm64     docker --platform             host is the other arch)
#   darwin/arm64    native go build              macOS + Xcode CLT
#   darwin/amd64    go build -arch x86_64        macOS + Xcode CLT
#
# Consequences, stated plainly:
#
#   * A macOS host can build all four (docker covers linux, clang's -arch
#     covers both darwin slices).
#   * A Linux host can build the two linux targets ONLY. Producing darwin
#     binaries from Linux needs a macOS SDK and an osxcross-style toolchain,
#     which this script deliberately does not ship. Those targets are skipped
#     with a warning rather than silently emitting something broken.
#   * Windows is not a target at all.
#
# The binaries are dynamically linked against libc and dlopen libduckdb at
# runtime; they are not standalone. See scripts/install-libduckdb.sh, and the
# README's install section.
#
# Usage: release-binaries.sh [dest-dir]

DEST_DIR="${1:-dist}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

GO_IMAGE="${GO_IMAGE:-golang:1.25-bookworm}"
GO_MODULE="github.com/turbolytics/sql-flow"
VERSION="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
COMMIT="${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
LDFLAGS="-X ${GO_MODULE}/internal/cli.Version=${VERSION} -X ${GO_MODULE}/internal/cli.Commit=${COMMIT}"

mkdir -p "$DEST_DIR"

echo "=== sqlflow release binaries ==="
echo "version: $VERSION"
echo "commit:  $COMMIT"
echo "host:    $(uname -s)/$(uname -m)"
echo "dest:    $DEST_DIR"
echo ""

built=()
skipped=()

# --- linux targets: built inside a container for that platform -------------
#
# The container supplies the linux C toolchain the host does not have. The
# module cache is shared with the host so a release build does not re-download
# the world, and the container runs as the invoking user so the artifacts are
# not left root-owned on a linux host.
build_linux() {
    local arch="$1"
    local platform="linux/${arch}"
    local out="sqlflow_${VERSION}_linux_${arch}"

    if ! command -v docker >/dev/null 2>&1; then
        echo "SKIP $platform: docker is not installed"
        skipped+=("$platform (no docker)")
        return
    fi

    echo "--- building $platform (docker $GO_IMAGE) ---"
    if ! docker run --rm --platform "$platform" \
        -u "$(id -u):$(id -g)" \
        -v "$PWD":/src \
        -v "$(go env GOMODCACHE)":/gomod \
        -e HOME=/tmp \
        -e GOCACHE=/tmp/gocache \
        -e GOMODCACHE=/gomod \
        -e GOTOOLCHAIN=auto \
        -e GOFLAGS=-buildvcs=false \
        -e CGO_ENABLED=1 \
        -w /src "$GO_IMAGE" \
        go build -ldflags "$LDFLAGS" -o "$DEST_DIR/$out" ./cmd/sqlflow/
    then
        # Most often: no binfmt handler registered for the non-native arch.
        echo "SKIP $platform: container build failed (is binfmt/qemu set up for $arch?)"
        skipped+=("$platform (build failed)")
        return
    fi

    built+=("$out")
}

# --- darwin targets: native, and clang's -arch for the other slice ---------
build_darwin() {
    local arch="$1"
    local out="sqlflow_${VERSION}_darwin_${arch}"

    if [ "$(uname -s)" != "Darwin" ]; then
        echo "SKIP darwin/$arch: needs a macOS host with the Xcode command line tools"
        skipped+=("darwin/$arch (not a macOS host)")
        return
    fi

    local clang_arch
    case "$arch" in
        amd64) clang_arch="x86_64" ;;
        arm64) clang_arch="arm64" ;;
        *) echo "unknown darwin arch $arch" >&2; exit 1 ;;
    esac

    echo "--- building darwin/$arch (native toolchain, -arch $clang_arch) ---"
    CGO_ENABLED=1 GOOS=darwin GOARCH="$arch" \
        CGO_CFLAGS="-arch $clang_arch" \
        CGO_LDFLAGS="-arch $clang_arch" \
        go build -ldflags "$LDFLAGS" -o "$DEST_DIR/$out" ./cmd/sqlflow/

    built+=("$out")
}

build_linux amd64
build_linux arm64
build_darwin arm64
build_darwin amd64

echo ""
echo "--- artifacts ---"
for b in "${built[@]:-}"; do
    [ -n "$b" ] || continue
    printf '%s\n  %s\n' "$b" "$(file -b "$DEST_DIR/$b")"
done

# Checksums cover whatever this host actually produced.
if [ "${#built[@]}" -gt 0 ]; then
    (cd "$DEST_DIR" && shasum -a 256 "${built[@]}" > SHA256SUMS)
    echo ""
    echo "checksums: $DEST_DIR/SHA256SUMS"
fi

if [ "${#skipped[@]}" -gt 0 ]; then
    echo ""
    echo "--- NOT built on this host ---"
    for s in "${skipped[@]}"; do
        echo "  $s"
    done
fi
