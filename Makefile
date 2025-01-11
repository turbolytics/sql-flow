.PHONY: install-tools
install-tools:
	$(shell mkdir -p /tmp/sqlflow/resultscache)

.PHONY: test
test: test-unit test-integration

.PHONY: test-unit
test-unit:
	pytest --ignore=tests/benchmarks --ignore=tests/integration tests

.PHONY: test-integration
test-integration:
	pytest tests/integration -s

.PHONY: start-backing-services
start-backing-services:
	docker-compose -f dev/kafka-single.yml up -d

.PHONY: docker-image
docker-image:
	docker build -t turbolytics/sql-flow .