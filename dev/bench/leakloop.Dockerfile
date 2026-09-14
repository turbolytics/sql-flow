# Linux runner for the component leak loops: Go and libduckdb at the version
# in DUCKDB_VERSION. The loops read RssAnon, which only Linux reports exactly.
#
#   docker build -f dev/bench/leakloop.Dockerfile -t sqlflow-leakloop .
#
# dev/bench/leakloops.sh builds it if it is missing.
FROM golang:1.25-bookworm

ENV GOTOOLCHAIN=auto
ENV GOFLAGS=-buildvcs=false

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip \
    && rm -rf /var/lib/apt/lists/*

COPY DUCKDB_VERSION /tmp/duckdb/DUCKDB_VERSION
COPY scripts/install-libduckdb.sh /tmp/duckdb/scripts/install-libduckdb.sh
RUN /tmp/duckdb/scripts/install-libduckdb.sh /usr/local/lib

ENV SQLFLOW_DUCKDB_LIB=/usr/local/lib/libduckdb.so
