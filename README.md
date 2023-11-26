# sql-flow
DuckDB for streaming data. 

SQL Flow enables SQL-based streaming transformations, powered by DuckDB.

SQL Flow enables writing SQL against a stream of kafka data.

SQL Flow has a number of goals:
- Make it trivial to use SQL for streaming data transformations.
- Support high performance kafka streaming.

What SQL Flow isn't:
- A stateful streaming engine like Flink. 


## Getting Started



## Development 

```
C_INCLUDE_PATH=/opt/homebrew/Cellar/librdkafka/2.3.0/include LIBRARY_PATH=/opt/homebrew/Cellar/librdkafka/2.3.0/lib pip install confluent-kafka
```

```
cd dev 
docker-compose -f kafka-single.yml up
```

```
python3 cmd/sql-flow.py run /Users/danielmican/code/github.com/turbolytics/sql-flow/dev/config/inferred_schema.yml

python publish-test-data.py
```

```
docker run -v $(PWD)/dev:/tmp/conf -v /tmp/sqlflow:/tmp/sqlflow sql-flow dev invoke /tmp/conf/config/inferred_schema.yml /tmp/conf/fixtures/simple.json
['{"city":"New York","city_count":28672}', '{"city":"Baltimore","city_count":28672}']
```
