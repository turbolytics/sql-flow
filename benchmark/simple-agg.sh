# delete kafka topic if exists
# docker exec -it kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic topic-simple-agg || true

# Publish benchmark data set
# python3 cmd/publish-test-data.py --num-messages=1000000 --topic="topic-simple-agg"

# consume until complete
/usr/bin/time -l python3 cmd/sql-flow.py run $(PWD)/dev/config/benchmarks/simple_agg.yml --max-msgs-to-process=1000000