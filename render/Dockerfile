# The published sqlflow image carries the matching libduckdb and sets
# SQLFLOW_DUCKDB_LIB. Pinned: a later release could change the config schema
# under this template, and a copy deployed from the button never rebuilds on
# its own. Must match the Makefile and docker-compose.yml.
#
# An argument so an unreleased build can be tried locally:
#   docker build --build-arg SQLFLOW_IMAGE=turbolytics/sql-flow:<tag> render
ARG SQLFLOW_IMAGE=turbolytics/sql-flow:v2026.09.18
FROM ${SQLFLOW_IMAGE}

# psql and pg_isready apply the migrations. curl sends the install events.
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pipeline.yml serve.yml /app/
COPY migrations /app/migrations
COPY bin /app/bin

RUN chmod +x /app/bin/*.sh

# Logs and window timestamps are UTC regardless of the host.
ENV TZ=UTC

# The ingest service's entrypoint. The API service overrides it with
# bin/serve.sh.
ENTRYPOINT ["/app/bin/entrypoint.sh"]
