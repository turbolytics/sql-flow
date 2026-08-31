# --- turbine (Go engine) -----------------------------------------------------
# DUCKDB_VERSION is pinned in one place, the file of the same name, and read
# from there by the container build, the benchmark script and CI.
DUCKDB_VERSION := $(shell tr -d '[:space:]' < DUCKDB_VERSION)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
TURBINE_IMAGE ?= turbolytics/turbine:$(VERSION)

GO_MODULE := github.com/turbolytics/turbine
GO_LDFLAGS := -X $(GO_MODULE)/internal/cli.Version=$(VERSION) \
	-X $(GO_MODULE)/internal/cli.Commit=$(GIT_COMMIT)

.PHONY: install-tools
install-tools:
	@echo "Installing tools..."
	@echo "creating resultscache directory... '/tmp/sqlflow/resultscache'"
	$(shell mkdir -p /tmp/sqlflow/resultscache)

.PHONY: setup-dev
setup-dev: install-tools

.PHONY: test
test: test-unit test-integration

.PHONY: test-unit
test-unit:
	PYICEBERG_HOME=$(shell pwd)/tests/config/ \
		pytest \
		--ignore=tests/benchmarks \
		--ignore=tests/integration \
		--ignore=tests/release \
		tests

.PHONY: test-image
test-image: docker-image
	pytest tests/release

.PHONY: test-integration
test-integration:
	PYICEBERG_HOME=$(shell pwd)/tests/config/ pytest tests/integration

.PHONY: test-release
test-release: docker-image
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

# Runs turbine inside the docker network to avoid Docker Desktop's slow
# host->container port-forwarding (which caps fetches at ~10-15MB/s)
.PHONY: benchmark-container
benchmark-container:
	./scripts/benchmark-container.sh $(NUM_MESSAGES) $(BATCH_SIZE)

# turbine needs cgo for the ADBC driver manager, so CGO_ENABLED is never off.
.PHONY: turbine
turbine:
	CGO_ENABLED=1 go build -ldflags "$(GO_LDFLAGS)" -o bin/turbine ./cmd/turbine/

# The build reads DUCKDB_VERSION itself; the label records which DuckDB ended up
# in the image so it can be read back off a pulled tag.
.PHONY: turbine-image
turbine-image:
	docker build -f Dockerfile.turbine \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(GIT_COMMIT) \
		--label org.opencontainers.image.version=$(VERSION) \
		--label org.opencontainers.image.revision=$(GIT_COMMIT) \
		--label io.turbolytics.duckdb.version=$(DUCKDB_VERSION) \
		-t $(TURBINE_IMAGE) .

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
	go test ./internal/...

.PHONY: docker-image
docker-image:
	@GIT_HASH=$$(git rev-parse --short HEAD) && \
	docker build --platform linux/amd64 -t turbolytics/sql-flow:$$GIT_HASH .

.PHONY: docker-image-multiarch
docker-image-multiarch:
	@GIT_HASH=$$(git rev-parse --short HEAD) && \
	docker build --platform linux/arm64 -t turbolytics/sql-flow:$$GIT_HASH .
	# docker buildx build --platform linux/arm64,linux/amd64 -t turbolytics/sql-flow:multiarch-$$GIT_HASH --push .