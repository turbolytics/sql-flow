# SQLFlow: DuckDB for Streaming Data. 

![Docker Pulls](https://img.shields.io/docker/pulls/turbolytics/sql-flow)

SQLFlow enables SQL-based stream-processing, powered by [DuckDB](https://duckdb.org/). SQLFlow embeds duckdb, supporting [kafka stream processing](https://kafka.apache.org/) logic using pure sql.

SQLFlow executes SQL against streaming data, such as Kafka or webhooks. Think of SQLFlow as a way to run sql against a continuous stream of data. The data outputs can be shipped to sinks, such as Kafka.

<img width="1189" alt="Screenshot 2024-12-31 at 7 22 55 AM" src="https://github.com/user-attachments/assets/1295e7eb-a0b8-4087-8aa4-cad75a0c8cfa" />

## SQLFlow Use-Cases

- **Streaming Data Transformations**: Clean data and types and publish the new data ([example config](https://github.com/turbolytics/sql-flow/blob/main/dev/config/examples/basic.agg.mem.yml)).
- **Stream Enrichment**: Add data an input stream and publish the new data ([example config](https://github.com/turbolytics/sql-flow/blob/main/dev/config/examples/enrich.yml)).
- **Data aggregation**: Aggregate input data batches to decrease data volume ([example config](https://github.com/turbolytics/sql-flow/blob/main/dev/config/examples/basic.agg.mem.yml)).
- **Tumbling Window Aggregation**: Bucket data into arbitrary time windows (such as "hour" or "10 minutes") ([example config](https://github.com/turbolytics/sql-flow/blob/main/dev/config/examples/tumbling.window.yml)).
- **Running SQL against the Bluesky Firehose**: Execute SQL against any webhook source, such as the [Bluesky firehose](https://docs.bsky.app/docs/advanced-guides/firehose) ([example config](https://github.com/turbolytics/sql-flow/blob/main/dev/config/examples/bluesky/bluesky.kafka.raw.yml))

## SQLFlow Features & Roadmap

- Sources
  - [x] Kafka Consumer using consumer groups
  - [x] Websocket input (for consuming bluesky firehose)
  - [ ] HTTP (for webhooks)
- Sinks
  - [x] Kafka Producer
  - [x] Stdout
  - [x] Local Disk
  - [ ] Postgres
  - [ ] S3
- Serialization
  - [x] JSON Input
  - [x] JSON Output
  - [x] Parquet Output
  - [x] Iceberg Output
- Handlers
  - [x] Memory Persistence
  - [ ] Disk Persistence
- [x] CSV Static Files for joinging static data during processing
- [x] Tumbling Window Aggregations
- [ ] Observability Metrics

## Getting Started

### Docker

[Docker is the easiest way to get started.](https://hub.docker.com/r/turbolytics/sql-flow)

- Pull the sql-flow docker image
```
docker pull turbolytics/sql-flow:latest
```

- Validate config by invoking it on test data
```
docker run -v $(pwd)/dev:/tmp/conf -v /tmp/sqlflow:/tmp/sqlflow turbolytics/sql-flow:latest dev invoke /tmp/conf/config/examples/basic.agg.yml /tmp/conf/fixtures/simple.json

['{"city":"New York","city_count":28672}', '{"city":"Baltimore","city_count":28672}']
```

- Start kafka locally using docker
```
docker-compose -f dev/kafka-single.yml up -d
```

- Publish test messages to kafka
```
python3 cmd/publish-test-data.py --num-messages=10000 --topic="input-simple-agg-mem"
```

- Start kafka consumer from inside docker-compose container
```
docker exec -it kafka1 kafka-console-consumer --bootstrap-server=kafka1:9092 --topic=output-simple-agg-mem
```

- Start SQLFlow in docker

```
docker run -v $(pwd)/dev:/tmp/conf -v /tmp/sqlflow:/tmp/sqlflow -e SQLFLOW_KAFKA_BROKERS=host.docker.internal:29092 turbolytics/sql-flow:latest run /tmp/conf/config/examples/basic.agg.mem.yml --max-msgs-to-process=10000
```

- Verify output in the kafka consumer
 
```
...
...
{"city":"San Francisco504","city_count":1}
{"city":"San Francisco735","city_count":1}
{"city":"San Francisco533","city_count":1}
{"city":"San Francisco556","city_count":1}
```

The `dev invoke` command enables testing a SQLFlow pipeline configuration on a batch of test data. This enables fast feedback local development before launching a SQLFlow consumer that reads from kafka.

## Configuration

The heart of SQLFlow is the pipeline configuration file. Each configuration file specifies:

- Kafka configuration 
- Pipeline configuration 
  - **Source**: Input configuration
  - **Handler**: SQL transformation
  - **Sink**: Output configuration

<img width="1085" alt="Screenshot 2024-12-30 at 9 08 57 AM" src="https://github.com/user-attachments/assets/66834849-b266-42f7-a125-7cbbb318d470" />

Every instance of SQLFlow needs a pipeline configuration file.

## Consuming Bluesky Firehose

SQLFlow supports DuckDB over websocket. Running SQL against the [Bluesky firehose](https://docs.bsky.app/docs/advanced-guides/firehose) is a simple configuration file:

<img width="1280" alt="bluesky firehose config" src="https://github.com/user-attachments/assets/86a46875-3cfa-46d3-ab08-1457c29115d9" />

The following command starts a bluesky consumer and prints every post to stdout:

```
docker run -v $(pwd)/dev/config/examples:/examples turbolytics/sql-flow:latest run /examples/bluesky/bluesky.raw.stdout.yml
```

![output](https://github.com/user-attachments/assets/185c6453-debc-439a-a2b9-ed20fdc82851)

[Checkout the configuration files here](https://github.com/turbolytics/sql-flow/tree/main/dev/config/examples/bluesky)

## Streaming to Iceberg

SQLFlow supports writing to Iceberg tables using [pyiceberg](https://py.iceberg.apache.org/). 

The following configuration writes to an Iceberg table using a local SQLite catalog:

- Initialize the SQLite iceberg catalog and test table
```
python3 cmd/setup-iceberg-local.py setup
created default.city_events
created default.bluesky_post_events
Catalog setup complete.
```

- Start Kafka Locally
```
docker-compose -f dev/kafka-single.yml up -d
```

- Publish Test Messages to Kafka
```
python3 cmd/publish-test-data.py --num-messages=5000 --topic="input-kafka-mem-iceberg"
```

- Run SQLFlow, which will read from kafka and write to the iceberg table locally
```
docker run \
  -e SQLFLOW_KAFKA_BROKERS=host.docker.internal:29092 \
  -e PYICEBERG_HOME=/tmp/iceberg/ \ 
  -v $(pwd)/dev/config/iceberg/.pyiceberg.yaml:/tmp/iceberg/.pyiceberg.yaml \
  -v /tmp/sqlflow/warehouse:/tmp/sqlflow/warehouse \
  -v $(pwd)/dev/config/examples:/examples \
  turbolytics/sql-flow:latest run /examples/kafka.mem.iceberg.yml --max-msgs-to-process=5000
```

- Verify iceberg data was written by querying it with duckdb
```
% duckdb
v1.1.3 19864453f7
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
D select count(*) from '/tmp/sqlflow/warehouse/default.db/city_events/data/*.parquet';
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│         5000 │
└──────────────┘
```




## Recipes

Coming Soon, until then checkout:

- [Benchmark configurations](./benchmark)
- [Unit Test configurations](./tests/)

#### Running multiple SQLFlow instances on the same filesystem 
#### Verifying a configuration locally 


## Development 

- Install python deps
```
pip install -r requirements.txt
pip install -r requirements.dev.txt

C_INCLUDE_PATH=/opt/homebrew/Cellar/librdkafka/2.3.0/include LIBRARY_PATH=/opt/homebrew/Cellar/librdkafka/2.3.0/lib pip install confluent-kafka
```

- Run tests
```
make test-unit
```
 

## [Benchmarks](https://github.com/turbolytics/sql-flow/wiki/Benchmarks)

The following table shows the performance of different test scenarios:


| Name                      | Throughput        | Max RSS Memory | Peak Memory Usage |
|---------------------------|-------------------|----------------|-------------------|
| Simple Aggregation Memory | 45,000 msgs / sec | 230 MiB        | 130 MiB           |
| Simple Aggregation Disk   | 36,000 msgs / sec | 256 MiB        | 102 MiB           |
| Enrichment                | 13,000 msgs /sec  | 368 MiB        | 124 MiB           |
| CSV Disk Join             | 11,500 msgs /sec  | 312 MiB        | 152 MiB           |
| CSV Memory Join           | 33,200 msgs / sec | 300 MiB        | 107 MiB           |
| In Memory Tumbling Window | 44,000 msgs / sec | 198 MiB        |  96 MiB           |

[More information about benchmarks are available in the wiki](https://github.com/turbolytics/sql-flow/wiki/Benchmarks). 

# Contact Us 

Like SQLFlow? Use SQLFlow? Feature Requests? Please let us know! danny@turbolytics.io
