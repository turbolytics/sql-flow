NUM_MESSAGES=${NUM_MESSAGES:-1000000}

echo "starting benchmark for kafka-mem-parquet-local with $NUM_MESSAGES messages"

docker exec -it kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic topic-local-parquet-sink || true

mkdir -p /tmp/sqlflow/test/local.parquet.sink.yml

python3 cmd/publish-test-data.py --num-messages=$NUM_MESSAGES --topic="topic-local-parquet-sink"

/usr/bin/time -l python3 cmd/sql-flow.py run $(PWD)/dev/config/examples/local.parquet.sink.yml --max-msgs-to-process=$NUM_MESSAGES
