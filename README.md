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

## Benchmarks

**Methodology**

Each test loads 1MM records into kafka. Each test executes sql-flow consumer until each message is processed. Each test captures the maximum resident memory during the benchmark, and the average throughput of message ingestion.

**System**

```
Hardware:
    Hardware Overview:
      Model Name: MacBook Pro
      Model Identifier: MacBookPro18,3
      Model Number: Z15G001X2LL/A
      Chip: Apple M1 Pro
      Total Number of Cores: 10 (8 performance and 2 efficiency)
      Memory: 32 GB
      Activation Lock Status: Enabled
```

### Simple Aggregate 

Performs a simple aggregate. Output is significantly 
smaller than input.

```
python3 cmd/publish-test-data.py --num-messages=1000000 --topic="topic-simple-agg"
/usr/bin/time -l python3 cmd/sql-flow.py run /Users/danielmican/code/github.com/turbolytics/sql-flow/dev/config/benchmarks/simple_agg.yml

36k messages / second
256 MiB Max - maximum resident set size
102 MiB Max - peak memory footprint
```

### Enriches

Performs an enrichment. Output is 1:1 records with input, but
each output record is enchanced with additional information.

```
python3 cmd/publish-test-data.py --num-messages=1000000 --topic="topic-enrich"

/usr/bin/time -l python3 cmd/sql-flow.py run /Users/danielmican/code/github.com/turbolytics/sql-flow/dev/config/benchmarks/enrich.yml

13k messages / second
368 MiB Max - maximum resident size
124 MiB - peak memory footprint
```