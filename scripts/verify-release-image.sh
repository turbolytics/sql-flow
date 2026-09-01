#!/usr/bin/env bash
set -euo pipefail

# Verifies a published image is what a release is supposed to be. Reads the
# registry rather than the local daemon: a single-arch publish, or a `latest`
# left pointing at an older release, both look correct locally.
#
# v1.0.0 shipped arm64-only because it was published from a mac with a plain
# `docker build`, and nothing checked. This is that check.
#
# Usage: verify-release-image.sh <image[:tag]> [latest-image[:tag]]
#
# The second argument is optional; when given, its manifest digest must equal
# the first's, which is what proves `latest` points at this release rather
# than merely being multi-arch in its own right.

IMAGE="${1:?usage: verify-release-image.sh <image:tag> [latest-image:tag]}"
LATEST="${2:-}"

REQUIRED_PLATFORMS="${REQUIRED_PLATFORMS:-linux/amd64 linux/arm64}"

# The buildx bundled with older Docker Desktop ignores --format, so the digest
# is read off the human-readable output instead. The first Digest line is the
# index digest, which is the one that identifies the multi-arch image.
digest_of() {
    docker buildx imagetools inspect "$1" 2>/dev/null | awk '/^Digest:/ { print $2; exit }'
}

platforms_of() {
    docker buildx imagetools inspect "$1" 2>/dev/null |
        awk '/^[[:space:]]*Platform:/ { print $2 }' | sort -u
}

fail() {
    echo "verify-release-image: $*" >&2
    exit 1
}

image_digest="$(digest_of "$IMAGE")"
[ -n "$image_digest" ] || fail "$IMAGE not found in the registry"
echo "$IMAGE"
echo "  digest:    $image_digest"

image_platforms="$(platforms_of "$IMAGE")"

# A single-arch publish has no manifest list, so it lists no platforms at all.
# That is the v1.0.0 failure exactly, and it deserves its own message rather
# than reading as "missing every platform".
[ -n "$image_platforms" ] || fail "$IMAGE is a single-arch image with no manifest list; expected: $REQUIRED_PLATFORMS
  It was probably published with a plain \`docker build\` + \`docker push\`,
  or retagged with \`docker tag\`, either of which flattens a manifest list."

echo "  platforms: $(echo "$image_platforms" | tr '\n' ' ')"

missing=""
for want in $REQUIRED_PLATFORMS; do
    echo "$image_platforms" | grep -qx "$want" || missing="$missing $want"
done
[ -z "$missing" ] || fail "$IMAGE is missing platform(s):$missing"

if [ -n "$LATEST" ]; then
    latest_digest="$(digest_of "$LATEST")"
    [ -n "$latest_digest" ] || fail "$LATEST not found in the registry"
    echo "$LATEST"
    echo "  digest:    $latest_digest"

    if [ "$latest_digest" != "$image_digest" ]; then
        fail "$LATEST does not point at $IMAGE
    $LATEST -> $latest_digest
    $IMAGE -> $image_digest
  Republish, or repoint it without rebuilding:
    docker buildx imagetools create -t $LATEST $IMAGE"
    fi
    echo "  -> matches $IMAGE"
fi

echo "verify-release-image: OK"
