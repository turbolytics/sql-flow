.PHONY: install-tools
install-tools:
	$(shell mkdir -p /tmp/sqlflow/resultscache)

.PHONY: test-unit
test-unit:
	pytest tests


.PHONY: start-backing-services
start-backing-services:
	docker-compose -f dev/kafka-single.yml up -d