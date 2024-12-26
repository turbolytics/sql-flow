# SQLFlow: DuckDB for streaming data. 

SQLFlow enables SQL-based stream-processing, powered by [DuckDB](https://duckdb.org/). SQLFlow enables writing kafka stream processing logic in pure sql.

SQLFlow supports:
- Kafka streaming - Writing a consumer that performs SQL based transformations and publishing the output to another kafka topic.
- JSON on the wire
- Writing stream transformations in pure SQL, powered by [DuckDB](https://duckdb.org/)
- Performant [librdkafka](https://github.com/confluentinc/librdkafka) [python consumer](https://github.com/confluentinc/confluent-kafka-python)

SQLFlow is currently not a good fit for:
- Wire protocols other than JSON

SQLFlow is a kafka consumer that embeds SQL for stream transformation:

<img width="754" alt="Screenshot 2023-11-26 at 8 16 47 PM" src="https://github.com/turbolytics/sql-flow/assets/151242797/419d8688-1d08-45ce-b245-1c2c886a3157">

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

The `dev invoke` command enables testing a sql-flow pipeline configuration on a batch of test data. This enables fast feedback local development before launching a sql-flow consumer that reads from kafka.

## Configuration

The heart of sql-flow is the pipeline configuration file. Each configuration file specifies:

- Kafka configuration 
- Pipeline configuration 
  - Input configuration
  - SQL transformation
  - Output configuration

<img width="1021" alt="Screenshot 2023-11-26 at 8 10 44 PM" src="https://github.com/turbolytics/sql-flow/assets/151242797/4f286fdc-ac2b-4809-acdb-1dc4d239f883">

Every instance of sql-flow needs a pipeline configuration file.

## Recipes

Coming Soon, until then checkout:

- [Benchmark configurations](./dev/config/benchmarks)
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
