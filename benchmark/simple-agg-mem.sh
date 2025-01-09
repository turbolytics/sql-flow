NUM_MESSAGES=${NUM_MESSAGES:-1000000}

echo "starting benchmark for simple-agg-mem with $NUM_MESSAGES messages"

# delete kafka topic if exists
docker exec -it kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic topic-simple-agg-mem || true

# Publish benchmark data set
python3 cmd/publish-test-data.py --num-messages=$NUM_MESSAGES --topic="topic-simple-agg-mem"

# consume until complete
/usr/bin/time -l python3 cmd/sql-flow.py run $(PWD)/dev/config/examples/basic.agg.mem.yml --max-msgs-to-process=$NUM_MESSAGES
