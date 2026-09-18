# syntax=docker/dockerfile:1

# The sqlflow image: the v1 Go engine. `docker build .` gets you this; the
# legacy Python engine's image lives in Dockerfile.python.
#
# Build with `make sqlflow-image`, which supplies the build args below.
#
# The builder runs on the build platform and cross-compiles for the target,
# so a multi-arch build never runs a compiler under emulation. Go handles the
# Go side; the cgo side is Arrow's C data interface and the ADBC driver
# manager, plain C and C++ that Debian's cross toolchain compiles. libduckdb
# is never linked, only dlopened, so the target's copy is downloaded as a
# file. The runtime stage only copies, so nothing runs under emulation at all.
ARG GO_IMAGE=golang:1.25-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS builder

# go.mod asks for a newer Go than the builder image ships, and the official Go
# images pin GOTOOLCHAIN=local; auto lets the build fetch the toolchain go.mod
# requires instead of failing.
ENV GOTOOLCHAIN=auto
# The source is copied in without its .git, so stamping VCS info would fail.
ENV GOFLAGS=-buildvcs=false

ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /src

# Kept separate from the target-specific install below so this layer, and the
# module download after it, are shared by every target of one build.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

ARG TARGETARCH
ARG BUILDARCH

# A target that is not the build arch needs its cross toolchain: gcc and g++
# for the cgo packages, and libc6-dev-<arch>-cross for their headers, which
# the compiler packages depend on.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
        case "$TARGETARCH" in \
            amd64) triple=x86-64-linux-gnu ;; \
            arm64) triple=aarch64-linux-gnu ;; \
            *) echo "unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; \
        esac; \
        apt-get update \
        && apt-get install -y --no-install-recommends "gcc-$triple" "g++-$triple"; \
    fi

# The version comes from DUCKDB_VERSION, the single place it is pinned. The
# script reads TARGETARCH and fetches the target's library.
COPY DUCKDB_VERSION ./DUCKDB_VERSION
COPY scripts/install-libduckdb.sh ./scripts/install-libduckdb.sh
RUN ./scripts/install-libduckdb.sh /out/duckdb

COPY cmd ./cmd
COPY internal ./internal

# CGO_ENABLED=1 is required: the ADBC driver manager reaches libduckdb through
# cgo, so a static pure-Go build cannot talk to DuckDB at all. The build cache
# is a mount so a rebuild recompiles only what changed.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
        case "$TARGETARCH" in \
            amd64) export CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ ;; \
            arm64) export CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ ;; \
        esac; \
    fi; \
    CGO_ENABLED=1 GOOS=linux GOARCH=$TARGETARCH go build \
    -ldflags "-X github.com/turbolytics/sql-flow/internal/buildinfo.Version=${VERSION} -X github.com/turbolytics/sql-flow/internal/buildinfo.Commit=${COMMIT}" \
    -o /out/sqlflow ./cmd/sqlflow/

FROM ${RUNTIME_IMAGE}

# ca-certificates for TLS to Kafka/S3/MotherDuck, copied from the builder
# rather than installed: the files are the same, and an apt-get here would be
# the one step left running under emulation. libduckdb.so needs libstdc++,
# which bookworm-slim already carries.
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /usr/share/ca-certificates /usr/share/ca-certificates

COPY --from=builder /out/sqlflow /usr/local/bin/sqlflow
COPY --from=builder /out/duckdb/libduckdb.so /usr/local/lib/libduckdb.so

ENV SQLFLOW_DUCKDB_LIB=/usr/local/lib/libduckdb.so

# glibc opens a malloc arena per thread that allocates, up to eight per core,
# and each arena keeps its own free pages. A cgo process on a small container
# pays for that: the Bluesky demo's Postgres upsert grew native memory 55 KB
# a flush with the default and 20 KB with two arenas, over 3,600 flushes on
# DuckDB v1.5.2 (#290). Two is the usual setting for a Go and cgo container.
ENV MALLOC_ARENA_MAX=2

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/sqlflow"]
