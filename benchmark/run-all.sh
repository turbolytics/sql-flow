NUM_MESSAGES=10000 ./benchmark/csv-disk-join.sh
NUM_MESSAGES=10000 ./benchmark/csv-mem-join.sh
NUM_MESSAGES=10000 ./benchmark/enrichment.sh
NUM_MESSAGES=10000 ./benchmark/kafka-mem-iceberg-local.sh
NUM_MESSAGES=10000 ./benchmark/kafka-mem-parquet-local.sh
NUM_MESSAGES=10000 ./benchmark/simple-agg-mem.sh
NUM_MESSAGES=10000 ./benchmark/simple-agg-disk.sh
NUM_MESSAGES=10000 ./benchmark/tumbling-window.sh
