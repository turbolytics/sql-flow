# sql-flow

SQL Flow enables SQL based streaming transformations. 

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
