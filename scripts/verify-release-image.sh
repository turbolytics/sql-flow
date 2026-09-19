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

fail() {
    echo "verify-release-image: $*" >&2
    exit 1
}

# One read of the registry, kept, so the digest and the platforms come from the
# same answer and the registry's own words survive.
#
# stderr used to go to /dev/null, which made every registry failure look
# identical and silent: under `set -e` the failing command substitution ended
# the script at the assignment below, before the "not found" message written
# for exactly that case could run. A release job failed this way in 1 second
# with an exit code and no output, and what the registry actually said -- 401,
# 429, no such tag -- is the whole of what a reader needs.
inspect_of() {
    local out
    if ! out="$(docker buildx imagetools inspect "$1" 2>&1)"; then
        echo "verify-release-image: cannot read $1 from the registry:" >&2
        printf '%s\n' "$out" | sed 's/^/  /' >&2
        return 1
    fi
    printf '%s\n' "$out"
}

# The buildx bundled with older Docker Desktop ignores --format, so the digest
# is read off the human-readable output instead. The first Digest line is the
# index digest, which is the one that identifies the multi-arch image.
digest_of() {
    printf '%s\n' "$1" | awk '/^Digest:/ { print $2; exit }'
}

platforms_of() {
    printf '%s\n' "$1" | awk '/^[[:space:]]*Platform:/ { print $2 }' | sort -u
}

image_inspect="$(inspect_of "$IMAGE")" ||
    fail "$IMAGE could not be read; the registry's error is above"
image_digest="$(digest_of "$image_inspect")"
[ -n "$image_digest" ] || fail "$IMAGE carries no index digest, so it is not a manifest list"
echo "$IMAGE"
echo "  digest:    $image_digest"

image_platforms="$(platforms_of "$image_inspect")"

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
    latest_inspect="$(inspect_of "$LATEST")" ||
        fail "$LATEST could not be read; the registry's error is above"
    latest_digest="$(digest_of "$latest_inspect")"
    [ -n "$latest_digest" ] || fail "$LATEST carries no index digest"
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

# The manifest listing an architecture does not mean that binary runs. The
# non-native one is built under emulation and is never executed during the
# build, so this is the first time it runs at all. The version it reports has
# to be the image's tag, which also catches a stale layer shipping an older
# binary under a new tag.
tag="${IMAGE##*:}"
for platform in $REQUIRED_PLATFORMS; do
    # Pulled separately so the run's output is the binary's, not the pull's.
    docker pull -q --platform "$platform" "$IMAGE" >/dev/null 2>&1 || true
    out="$(docker run --rm --platform "$platform" "$IMAGE" version 2>&1)" ||
        fail "$IMAGE does not run on $platform:
$out"
    echo "$out" | grep -q "^sqlflow ${tag}$" ||
        fail "$IMAGE on $platform reports the wrong version; expected 'sqlflow ${tag}':
$out"
    echo "  runs on $platform: $(echo "$out" | head -1)"
done

echo "verify-release-image: OK"
