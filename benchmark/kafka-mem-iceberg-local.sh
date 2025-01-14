NUM_MESSAGES=${NUM_MESSAGES:-1000000}

echo "starting benchmark for kafka-mem-iceberg-local with $NUM_MESSAGES messages"

# delete kafka topic if exists
docker exec -it kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic input-kafka-mem-iceberg || true

# initialize iceberg
python3 cmd/setup-iceberg-local.py setup

# Publish benchmark data set
python3 cmd/publish-test-data.py --num-messages=$NUM_MESSAGES --topic="input-kafka-mem-iceberg"

# consume until complete
/usr/bin/time -l python3 cmd/sql-flow.py run $(PWD)/dev/config/examples/kafka.mem.iceberg.yml --max-msgs-to-process=$NUM_MESSAGES
