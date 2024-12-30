NUM_MESSAGES=${NUM_MESSAGES:-1000000}

# delete kafka topic if exists
docker exec -it kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic topic-csv-mem-join || true

# Publish benchmark data set
python3 cmd/publish-test-data.py --num-messages=$NUM_MESSAGES --topic="topic-csv-mem-join"

# consume until complete
SQLFLOW_STATIC_ROOT=$(PWD)/dev /usr/bin/time -l python3 cmd/sql-flow.py run $(PWD)/dev/config/examples/csv.mem.join.yml --max-msgs-to-process=$NUM_MESSAGES
