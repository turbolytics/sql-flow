# Redis Support

## Context

SQLFlow would like to enable redis's rich feature set for use during SQL stream processing. Redis ships with a ton of extremely useful datatypes, such as counting, sets and probabilistic data types. We would like enable SQLFlow to leverage these datatypes. 

A couple of concerns exist:
- Redis interactions enabled in the context of SQL.
- Redis interactions enabled in the context of stream processing.
- Writing to redis vs reading data from redis. 

## Decision

*What is the change that we're proposing and/or doing?*

### Redis table approach 

This approach establishes a magic table named `redis` that can only be written to.
#### Pipeline configuration

```
kafka:
    ...

redis:
    # table name for reference in SQLFlow queries
    table_name: <table_name> 
    host: <str>
    port: <int>
    database: <int>
    
pipeline:
    ...
```


#### Backing Redis Table

```sql
CREATE TABLE redis (
    command ENUM(... <all supported write commands>),
    key VARCHAR,
    
    # how to handle multiple values? 
    vint INTEGER
)
```

#### Incrementing a Counter

```sql
INSERT INTO <NAME>
SELECT
    'INCRBY' as command,
    'city-' || properties.city as key,
    count(*) as vint 
FROM batch
GROUP BY
    city
ORDER BY city DESC
```


#### Getting a value for referencing in query


#### Get and set

**Caveats**: Consistency, distributed transaction
- Batch - GET
- Batch - SET
- X Batch Fail

SET is incorrect


#### Implementation

- Create redis table in memory during initialization
- After each batch, check the state of redis table
- Iterate redis table and flush 
- ACK Batch



## Consequences

*What becomes easier or more difficult to do because of this change?*

- Strict Separation of reads and writes 
- Loss of pipelining, composite actions
-  
