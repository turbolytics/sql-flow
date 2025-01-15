.PHONY: install-tools
install-tools:
	$(shell mkdir -p /tmp/sqlflow/resultscache)

.PHONY: test
test: test-unit test-integration

.PHONY: test-unit
test-unit:
	PYICEBERG_HOME=$(shell pwd)/tests/config/ pytest --ignore=tests/benchmarks --ignore=tests/integration tests

.PHONY: test-image
test-image: docker-image
	pytest tests/release

.PHONY: test-integration
test-integration:
	PYICEBERG_HOME=$(shell pwd)/tests/config/ pytest tests/integration

.PHONY: start-backing-services
start-backing-services:
	docker-compose -f dev/kafka-single.yml up -d

.PHONY: docker-image
docker-image:
	@GIT_HASH=$$(git rev-parse --short HEAD) && \
	docker build --platform linux/amd64 -t turbolytics/sql-flow:$$GIT_HASH .