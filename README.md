# SQLFlow: DuckDB for Streaming Data. 

SQLFlow enables SQL-based stream-processing, powered by [DuckDB](https://duckdb.org/). SQLFlow embeds duckdb, supporting [kafka stream processing](https://kafka.apache.org/) logic using pure sql.

SQLFlow executes SQL against streaming data, such as Kafka or webhooks. Think of SQLFlow as a way to run sql against a continuous stream of data. The data outputs can be shipped to sinks, such as Kafka.

<img width="1189" alt="Screenshot 2024-12-31 at 7 22 55 AM" src="https://github.com/user-attachments/assets/1295e7eb-a0b8-4087-8aa4-cad75a0c8cfa" />

## SQLFlow Use-Cases

- **Streaming Data Transformations**: Clean data and types and publish the new data ([example config](https://github.com/turbolytics/sql-flow/blob/main/dev/config/examples/basic.agg.mem.yml)).
- **Stream Enrichment**: Add data an input stream and publish the new data ([example config](https://github.com/turbolytics/sql-flow/blob/main/dev/config/examples/enrich.yml)).
- **Data aggregation**: Aggregate input data batches to decrease data volume ([example config](https://github.com/turbolytics/sql-flow/blob/main/dev/config/examples/basic.agg.mem.yml)).
- **Tumbling Window Aggregation**: Bucket data into arbitrary time windows (such as "hour" or "10 minutes") ([example config](https://github.com/turbolytics/sql-flow/blob/main/dev/config/examples/tumbling.window.yml)).
- **Running SQL against the Bluesky Firehose**: Execute SQL against any webhook source, such as the [Bluesky firehose](https://docs.bsky.app/docs/advanced-guides/firehose).

## SQLFlow Features

- Kafka Stream Consumers/Producers: Use kafka streaming primitives, such as consumer groups, to scale processing horizontally.
- Websocket Stream Consumer: Run SQL against a stream of websocket payloads, such as the Bluesky Firehose
- Configurable Serialization/Encodings: Such as JSON on the wire.
- Stream transformations in pure SQL, powered by [DuckDB](https://duckdb.org/)
- High Performance: SQLFlow is benchmarked to process 10's of thousands of messages per second thanks to [DuckDB](https://duckdb.org/), [librdkafka](https://github.com/confluentinc/librdkafka), and [confluent python](https://github.com/confluentinc/confluent-kafka-python)
- Tumbling Window Aggregation: Aggregate data on fixed intervals, and produce data once those intervals completed. This allows for "hourly" or "10-minute" rollups.
- Join streaming data with any CSV-based data using SQLFlow static tables. 

## SQLFlow Roadmap 

- [x] Kafka Consumer using consumer groups
- [x] Kafka Producer
- [x] JSON Input
- [x] JSON Output
- [x] Memory Persistence 
- [x] CSV Static Files for joinging static data during processing
- [x] Tumbling Window Aggregations
- [x] Websocket input (for consuming bluesky firehose)
- [ ] Duckdb extensions for outputs (https, parquet, postgres, etc)
- [ ] HTTP input for webhook stremms
- [ ] Disk Persistence
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
docker run -v $(PWD)/dev:/tmp/conf -v /tmp/sqlflow:/tmp/sqlflow turbolytics/sql-flow:latest dev invoke /tmp/conf/config/examples/basic.agg.yml /tmp/conf/fixtures/simple.json

['{"city":"New York","city_count":28672}', '{"city":"Baltimore","city_count":28672}']
```

- Start kafka locally using docker
```
docker-compose -f dev/kafka-single.yml up -d
```

- Publish test messages to kafka
```
python3 cmd/publish-test-data.py --num-messages=10000 --topic="topic-local-docker"
```

- Start kafka consumer from inside docker-compose container
```
docker exec -it kafka1 kafka-console-consumer --bootstrap-server=kafka1:9092 --topic=output-local-docker
```

- Start SQLFlow in docker

```
docker run -v $(PWD)/dev:/tmp/conf -v /tmp/sqlflow:/tmp/sqlflow turbolytics/sql-flow:latest run /tmp/conf/config/local.docker.yml
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

Invoke sql-flow using the configuration listed above:

![output](https://github.com/user-attachments/assets/185c6453-debc-439a-a2b9-ed20fdc82851)

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
pytests tests
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
