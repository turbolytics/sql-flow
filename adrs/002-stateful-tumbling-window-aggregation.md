# Stateful Stream Processing through Tumbling Window Support

## Context

sql-flow aims to add support for stateful stream processing, starting with support for aggregations over tumbling windows. Flink is the industry standard in stateful stream processing, but is very heavy weight in terms of operations. Very few mature, well tested alternatives exist:
- Goka - https://github.com/lovoo/goka
- Benthos - https://www.benthos.dev/
- Others - https://github.com/manuzhang/awesome-streaming

The goal is to add support to sql-flow for stateful stream processining using only kafka and duckdb, backed by local disk. 

## Decision

*What is the change that we're proposing and/or doing?*

Adding support for stateful stream processing backed by duckdb disk-based databases. 


### Implementation

This ADR targets tumbling window stream aggregations. Tumbling widow aggregations have a couple of components:

- Aggregation definitions
- State
- Window interval (flushing)
- Sink

What happens if disk becomes too full? 
What is the consistency model for aggregations? 

Supported Windowing Operations
- Tumbling Window
- Additive aggregations (COUNT, SUM)
 

#### Flow
- User defines window config
  - Window type 
  - Window Size
  - Window Aggregation
- User defines the sql to generate the windowed data
- SQLFlow invokes the sql for each record
  - SQLFlow accumulates the data 
  - It aggregates each record and inserts it into the aggregation table
   

## Consequences
- Careful attention to disk growth 
- Management of background processes / threads 



## References
- https://duckdb.org/docs/connect/concurrency
- https://flink.apache.org/2015/12/04/introducing-stream-windows-in-apache-flink/
- https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/operators/windows/#window-lifecycle
- https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/operators/windows/#window-assigners
- https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/operators/windows/#allowed-lateness
- https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/operators/windows/#window-functions
