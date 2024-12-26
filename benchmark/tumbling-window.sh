# delete kafka topic if exists
docker exec -it kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic tumbling-window || true

# Publish benchmark data set
python3 cmd/publish-test-data.py --num-messages=1000000 --topic="tumbling-window"

# consume until complete
/usr/bin/time -l python3 cmd/sql-flow.py run $(PWD)/dev/config/examples/tumbling.window.yml --max-msgs-to-process=1000000
