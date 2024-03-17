# Stateful Stream Processing through Tumbling Window Support

## Context

sql-flow aims to add support for stateful stream processing, starting with support for aggregations over tumbling windows. Flink is the industry standard in stateful stream processing, but is very heavy weight in terms of operations. Very few mature, well tested alternatives exist:
- Goka - https://github.com/lovoo/goka
- Benthos - https://www.benthos.dev/
- Others - https://github.com/manuzhang/awesome-streaming

The goal is to add support to sql-flow for stateful stream processining using only kafka and duckdb, backed by local disk. 

## Decision

*What is the change that we're proposing and/or doing?*






### Implementation



## Consequences

