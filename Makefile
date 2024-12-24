.PHONY: install-tools
install-tools:
	$(shell mkdir -p /tmp/sqlflow/resultscache)

.PHONY: test-unit
test-unit:
	pytest --ignore=tests/benchmarks --ignore=tests/integration tests


.PHONY: start-backing-services
start-backing-services:
	docker-compose -f dev/kafka-single.yml up -d