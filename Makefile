# --- sqlflow (Go engine) -----------------------------------------------------
# DUCKDB_VERSION is pinned in one place, the file of the same name, and read
# from there by the container build, the benchmark script and CI.
DUCKDB_VERSION := $(shell tr -d '[:space:]' < DUCKDB_VERSION)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
SQLFLOW_IMAGE ?= turbolytics/sql-flow:$(VERSION)
DIST_DIR ?= dist

GO_MODULE := github.com/turbolytics/sql-flow
GO_LDFLAGS := -X $(GO_MODULE)/internal/cli.Version=$(VERSION) \
	-X $(GO_MODULE)/internal/cli.Commit=$(GIT_COMMIT)

.PHONY: install-tools
install-tools:
	@echo "Installing tools..."
	@echo "creating resultscache directory... '/tmp/sqlflow/resultscache'"
	$(shell mkdir -p /tmp/sqlflow/resultscache)

.PHONY: setup-dev
setup-dev: install-tools

# The Go checks and the image tests. There is no Python suite: the engine it
# covered is gone, and what remains of the package is harness for the image
# tests below.
.PHONY: test
test: test-go test-release

# Functional tests against the shipped container image: entrypoint, CLI
# surface, baked-in libduckdb, and a real pipeline through Kafka. Runs against
# the Go engine's image, which is what v1 publishes; point SQLFLOW_IMAGE at
# another tag to test that one instead.
.PHONY: test-image
test-image: sqlflow-image
	SQLFLOW_IMAGE=$(SQLFLOW_IMAGE) pytest tests/release

# Runs every suite and regenerates the matrix from what they report.
#
# This is the local convenience target: one command, everything runs, the
# matrix is current. CI does not use it. There the jobs that own the tests
# write their reports and `coverage-check` reads them, so coverage costs
# seconds instead of a second image build and a second release run.
#
# Depends on the image because the release suite runs against it. Without that
# dependency every release test errors in collection, and the `-` prefixes
# swallow it: the matrix regenerates with every release row marked failing.
#
# Three passes, one per level. -short is the unit pass and runs anywhere; the
# integration pass runs the tests that provision a real service; the release
# pass runs against the built image.
.PHONY: coverage-matrix
coverage-matrix: sqlflow-image
	@mkdir -p .coverage
	-CGO_ENABLED=1 go test -short -json ./... > .coverage/go.json 2>&1
	-CGO_ENABLED=1 go test -json -run '^TestIntegration' ./... \
		> .coverage/go-integration.json 2>&1
	-SQLFLOW_PYTEST_JSON=$(shell pwd)/.coverage/pytest.json \
		SQLFLOW_IMAGE=$(SQLFLOW_IMAGE) \
		TC_KAFKA_LIMIT_BROKER_TO_FIRST_HOST=true \
		pytest tests/release -q
	@$(MAKE) --no-print-directory coverage-write

# Renders the matrix from reports that already exist. Runs no tests.
.PHONY: coverage-write
coverage-write:
	python3 scripts/coverage_matrix.py \
		--go .coverage/go.json \
		--go-integration .coverage/go-integration.json \
		--pytest .coverage/pytest.json --write

# The merge gate, in three parts. Runs no tests either: it reads the reports
# the suites already wrote, which is what lets CI run it last and in seconds.
#
# Gaps: a feature missing a level it requires fails the build. There is no
# baseline and no escape hatch -- a gap is closed by a test, or by the registry
# honestly no longer requiring that level.
#
# Stale: the checked-in matrix must match what the suites just reported, the
# way a golden file does. That is what makes a coverage change show up in
# review rather than nowhere.
#
# Failures: a suite failure reaches the gap check as a failing feature, so gaps
# are checked first and the order carries information -- "sink.kafka: release
# is failing" is the truth, where a stale-matrix error would send the reader to
# regenerate a file that was never the problem. A failing test matching no
# declared feature reaches neither check, and the last part is its backstop.
.PHONY: coverage-check
coverage-check: coverage-write
	python3 scripts/coverage_matrix.py \
		--go .coverage/go.json \
		--go-integration .coverage/go-integration.json \
		--pytest .coverage/pytest.json --check
	@! grep -hq '"Action":"fail"' .coverage/go.json .coverage/go-integration.json || { \
		echo "" >&2; \
		echo "A Go test failed:" >&2; \
		grep -ho '"Action":"fail","Test":"[^"]*"' \
			.coverage/go.json .coverage/go-integration.json \
			| sed 's/.*"Test":"/  /; s/"$$//' | sort -u >&2; \
		exit 1; \
	}
	@! grep -q '"outcome": "failed"' .coverage/pytest.json || { \
		echo "" >&2; \
		echo "A release test failed; see .coverage/pytest.json" >&2; \
		exit 1; \
	}
	@# Last, because it is the least specific. A failing test changes the
	@# matrix too, so checking staleness first reports "regenerate the file"
	@# for a problem no regeneration fixes.
	@git diff --exit-code docs/coverage/matrix.json docs/coverage/matrix.md || { \
		echo ""; \
		echo "The coverage matrix is out of date."; \
		echo "Run 'make coverage-matrix' and commit the result."; \
		exit 1; \
	}

# Everything, then the gate. The local equivalent of a full CI run.
.PHONY: coverage-matrix-check
coverage-matrix-check: coverage-matrix coverage-check

# Writes its report alongside running, so the coverage gate can read this run
# rather than repeat it. The file is cheap and unconditional: a suite whose
# report exists only when someone remembered a flag is a suite the gate
# silently stops seeing.
.PHONY: test-release
test-release: sqlflow-image
	@mkdir -p .coverage
	SQLFLOW_PYTEST_JSON=$(shell pwd)/.coverage/pytest.json \
	SQLFLOW_IMAGE=$(SQLFLOW_IMAGE) \
	TC_KAFKA_LIMIT_BROKER_TO_FIRST_HOST=true \
	pytest tests/release

.PHONY: start-backing-services
start-backing-services:
	docker-compose -f dev/kafka-single.yml up -d

.PHONY: stop-backing-services
stop-backing-services:
	docker-compose -f dev/kafka-single.yml down --remove-orphans

.PHONY: benchmark
benchmark:
	./scripts/benchmark.sh $(NUM_MESSAGES) $(BATCH_SIZE)

# Runs sqlflow inside the docker network to avoid Docker Desktop's slow
# host->container port-forwarding (which caps fetches at ~10-15MB/s)
.PHONY: benchmark-container
benchmark-container:
	./scripts/benchmark-container.sh $(NUM_MESSAGES) $(BATCH_SIZE) $(CONFIG)

# sqlflow needs cgo for the ADBC driver manager, so CGO_ENABLED is never off.
.PHONY: sqlflow
sqlflow:
	CGO_ENABLED=1 go build -ldflags "$(GO_LDFLAGS)" -o bin/sqlflow ./cmd/sqlflow/

# Cross-compiled release binaries for linux/darwin x amd64/arm64.
#
# sqlflow CANNOT be cross-compiled the usual way. The ADBC driver manager is a
# cgo package, so CGO_ENABLED=0 fails to compile outright ("undefined:
# drivermgr.Driver") and CGO_ENABLED=1 needs a C toolchain for the target OS,
# which `GOOS=linux go build` on a mac does not have. So: linux targets are
# built inside a container for that platform, and the two darwin slices are
# built natively with clang's -arch.
#
# A macOS host with docker produces all four. A LINUX HOST PRODUCES ONLY THE
# TWO LINUX TARGETS -- darwin binaries would need a macOS SDK and an
# osxcross-style toolchain, which is not shipped here. Unbuildable targets are
# reported as skipped, not faked.
#
# The binaries dlopen libduckdb at runtime and are not standalone; see
# scripts/install-libduckdb.sh.
.PHONY: release-binaries
release-binaries:
	./scripts/release-binaries.sh $(DIST_DIR)

# The build reads DUCKDB_VERSION itself; the label records which DuckDB ended up
# in the image so it can be read back off a pulled tag.
.PHONY: sqlflow-image
sqlflow-image:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(GIT_COMMIT) \
		--label org.opencontainers.image.version=$(VERSION) \
		--label org.opencontainers.image.revision=$(GIT_COMMIT) \
		--label io.turbolytics.duckdb.version=$(DUCKDB_VERSION) \
		-t $(SQLFLOW_IMAGE) .

# Publishes the Go engine's image. This is the ONLY target that pushes, and a
# bare version tag on turbolytics/sql-flow means the Go engine as of v1.
#
# Multi-arch is the point of the target existing. v1.0.0 was published by hand
# from a mac with a plain `docker build`, which produces a single-arch image, so
# it went out arm64-only and could not run on amd64 at all. buildx runs the
# whole Dockerfile once per platform against a platform-matching golang image;
# install-libduckdb.sh branches on `uname -m`, so each arch fetches its own
# libduckdb without any change here. CGO_ENABLED=1 rules out cross-compiling, so
# the foreign arch builds under emulation -- expect the amd64 `go build` to take
# several minutes on an arm64 host.
#
# Run `make test-image` before releasing; the guards below only cover release
# integrity, not correctness.
RELEASE_PLATFORMS ?= linux/amd64,linux/arm64
BUILDX_BUILDER ?= sqlflow-release
# Set RELEASE_LATEST=0 when re-publishing an older tag, so it does not claim
# `latest` from a newer release.
RELEASE_LATEST ?= 1
# Override with --output=type=cacheonly for a dry run that builds but publishes
# nothing.
RELEASE_OUTPUT ?= --push

.PHONY: release-image
release-image:
	@test -z "$$(git status --porcelain)" || { \
		echo "release-image: working tree is dirty, so $(VERSION) would not be reproducible" >&2; \
		echo "               commit or stash, then re-run" >&2; exit 1; }
	@# A clean, tagged HEAD is not enough: v1.0.5 was first tagged and published
	@# from a feature branch, so the image shipped unmerged commits and lacked
	@# the fixes its own tag message described. The release must be on main.
	@git fetch -q origin main && git merge-base --is-ancestor HEAD origin/main || { \
		echo "release-image: HEAD is not on origin/main, so $(VERSION) would ship unmerged commits" >&2; \
		echo "               git checkout main && git pull --ff-only origin main, then tag and re-run" >&2; \
		exit 1; }
	@git describe --exact-match --tags HEAD >/dev/null 2>&1 || { \
		echo "release-image: HEAD is not tagged (VERSION=$(VERSION))" >&2; \
		echo "               tag the release first: git tag -a vX.Y.Z -m ... && git push origin vX.Y.Z" >&2; \
		exit 1; }
	@docker buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || \
		docker buildx create --name $(BUILDX_BUILDER) --driver docker-container >/dev/null
	docker buildx build \
		--builder $(BUILDX_BUILDER) \
		--platform $(RELEASE_PLATFORMS) \
		-f Dockerfile \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(GIT_COMMIT) \
		--label org.opencontainers.image.version=$(VERSION) \
		--label org.opencontainers.image.revision=$(GIT_COMMIT) \
		--label io.turbolytics.duckdb.version=$(DUCKDB_VERSION) \
		-t $(SQLFLOW_IMAGE) \
		$(if $(filter 1,$(RELEASE_LATEST)),-t turbolytics/sql-flow:latest) \
		$(RELEASE_OUTPUT) .

# Reads the registry back, because a single-arch publish looks identical to a
# good one locally, and so does a `latest` still pointing at an older release.
.PHONY: release-image-verify
release-image-verify:
	./scripts/verify-release-image.sh $(SQLFLOW_IMAGE) \
		$(if $(filter 1,$(RELEASE_LATEST)),turbolytics/sql-flow:latest)

# Fetches the pinned libduckdb for linux into bin/; only useful for linux hosts
# and containers, macOS development uses the Homebrew install.
.PHONY: libduckdb
libduckdb:
	./scripts/install-libduckdb.sh bin

# The checks CI runs against the Go engine. Kafka-backed integration tests are
# deliberately excluded; these are unit tests only.
.PHONY: test-go
test-go:
	go build ./...
	go vet ./...
	@test -z "$$(gofmt -l internal/ cmd/)" || { echo "gofmt needed:"; gofmt -l internal/ cmd/; exit 1; }
	go test ./...

# The Python engine's image targets are gone with the engine. Published
# python-* tags are still on Docker Hub; reproducing one means checking out a
# commit from before this change.