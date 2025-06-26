import json
import os
import shutil
import tempfile
import unittest
import socket

import pytest
import pyarrow.dataset as ds
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestampType, StringType
from testcontainers.kafka import KafkaContainer

from sqlflow.config import new_from_path
from sqlflow.fixtures import KafkaFaker
from sqlflow import settings
from sqlflow.kafka import delete_topics, delete_consumer_groups, read_all_kafka_messages
from sqlflow.lifecycle import start


@pytest.fixture(scope="module")
def bootstrap_server():
    # Start the Kafka container
    with KafkaContainer() as kafka:
        yield kafka.get_bootstrap_server()


def test_kafka_mem_iceberg(bootstrap_server):
    num_messages = 5000
    in_topic = 'input-kafka-mem-iceberg'
    group_id = 'test_kafka_mem_iceberg'
    catalog_name = 'integration_test_kafka_mem_iceberg'
    table_name = 'default.city_events'

    warehouse_path = os.path.join(
        settings.SQL_RESULTS_CACHE_DIR,
        'integration',
        'test_kafka_mem_iceberg',
    )

    try:
        shutil.rmtree(warehouse_path)
    except FileNotFoundError:
        pass

    os.makedirs(warehouse_path)

    # Set up the catalog
    catalog = SqlCatalog(
        catalog_name,
        **{
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    catalog.create_namespace("default")

    schema = Schema(
        NestedField(field_id=1, name="timestamp", field_type=TimestampType(), required=False),
        NestedField(field_id=2, name="city", field_type=StringType(), required=False),
    )

    iceberg_table = catalog.create_table(
       table_name,
        schema=schema,
    )

    delete_topics([in_topic], bootstrap_server)
    delete_consumer_groups([group_id], bootstrap_server)
    kf = KafkaFaker(
        bootstrap_servers=bootstrap_server,
        num_messages=num_messages,
        topic=in_topic,
    )
    kf.publish()

    conf = new_from_path(
        path=os.path.join(settings.CONF_DIR, 'examples', 'kafka.mem.iceberg.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
            'catalog_name': catalog_name,
            'table_name': table_name,
        },
    )
    stats = start(conf, max_msgs=num_messages)

    iceberg_table.refresh()
    read_table = iceberg_table.scan().to_arrow()
    assert len(read_table) == num_messages


def test_local_parquet_sink(bootstrap_server):
    num_messages = 2000
    in_topic = 'topic-local-parquet-sink'
    group_id = 'test_local_parquet_sink'

    delete_topics([in_topic], bootstrap_server)
    delete_consumer_groups([group_id], bootstrap_server)
    kf = KafkaFaker(
        bootstrap_servers=bootstrap_server,
        num_messages=num_messages,
        topic=in_topic,
    )
    kf.publish()

    with tempfile.TemporaryDirectory() as temp_dir:
        conf = new_from_path(
            path=os.path.join(settings.CONF_DIR, 'examples', 'local.parquet.sink.yml'),
            setting_overrides={
                'kafka_brokers': bootstrap_server,
                'sink_base_path': temp_dir,
                'batch_size': 1000,
            },
        )

        stats = start(conf, max_msgs=num_messages)
        assert stats.num_messages_consumed == num_messages

        files = os.listdir(temp_dir)
        parquet_files = [f for f in files if f.endswith('.parquet')]
        assert len(parquet_files) == 2

        # Read the Parquet file and verify the content
        dataset = ds.dataset(temp_dir, format="parquet")
        table = dataset.to_table()
        total_records = sum(r['num_records'] for r in table.to_pylist())
        assert 2000 == total_records, f"Expected 2000 records, but got {total_records}"


def test_basic_agg_mem(bootstrap_server):
    num_messages = 1000
    in_topic = 'input-simple-agg-mem'
    out_topic = 'output-simple-agg-mem'

    delete_topics([in_topic, out_topic], bootstrap_server)
    delete_consumer_groups(['test_basic_agg_mem'], bootstrap_server)

    kf = KafkaFaker(
        bootstrap_servers=bootstrap_server,
        num_messages=num_messages,
        topic=in_topic,
    )
    kf.publish()

    conf = new_from_path(
        path=os.path.join(settings.CONF_DIR, 'examples', 'basic.agg.mem.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
        },
    )

    stats = start(conf, max_msgs=num_messages)
    assert stats.num_messages_consumed == num_messages
    print(stats)

    messages = read_all_kafka_messages(bootstrap_server, out_topic)
    assert 5 == len(messages)


def test_basic_agg_mem_ignore_invalid(bootstrap_server):
    num_messages = 10
    in_topic = 'input-simple-agg-mem-ignore'
    out_topic = 'output-simple-agg-mem-ignore'

    delete_topics([in_topic, out_topic], bootstrap_server)
    delete_consumer_groups(['test_basic_agg_mem'], bootstrap_server)

    conf = {
        'bootstrap.servers': bootstrap_server,
        'client.id': socket.gethostname()
    }
    producer = Producer(conf)
    for i in range(5):
        producer.produce(in_topic, value='invalid!')

    for i in range(5):
        producer.produce(in_topic, value=json.dumps({
            'properties': {
                'city': 'test',
            }
        }))

    producer.flush()

    conf = new_from_path(
        path=os.path.join(settings.CONF_DIR, 'examples', 'basic.agg.mem.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
            'SQLFLOW_SOURCE_ERROR_POLICY': 'ignore',
            'SQLFLOW_INPUT_TOPIC': in_topic,
            'SQLFLOW_OUTPUT_TOPIC': out_topic,
            'SQLFLOW_BATCH_SIZE': 1,
        },
    )

    stats = start(conf, max_msgs=num_messages)
    assert stats.num_messages_consumed == num_messages
    print(stats)

    messages = read_all_kafka_messages(bootstrap_server, out_topic)
    assert 5 == len(messages)


def test_csv_mem_join(bootstrap_server):
    num_messages = 1000
    in_topic = 'topic-csv-mem-join'
    out_topic = 'output-csv-mem-join'
    group_id = 'test_csv_mem_join'

    delete_topics([in_topic, out_topic], bootstrap_server)
    delete_consumer_groups([group_id], bootstrap_server)

    kf = KafkaFaker(
        bootstrap_servers=bootstrap_server,
        num_messages=num_messages,
        topic=in_topic,
    )
    kf.publish()

    conf = new_from_path(
        path=os.path.join(settings.CONF_DIR, 'examples', 'csv.mem.join.yml'),
        setting_overrides={
            'kafka_brokers': bootstrap_server,
            'STATIC_ROOT': settings.DEV_DIR,
        },
    )

    stats = start(conf, max_msgs=num_messages)
    assert stats.num_messages_consumed == num_messages
    print(stats)
    messages = read_all_kafka_messages(bootstrap_server, out_topic)

    assert len(messages) == 1000, f"Expected 1000 messages, but got {len(messages)}"


def test_enrichment(bootstrap_server):
    num_messages = 1000
    in_topic = 'topic-enrich'
    out_topic = 'output-enrich'
    group_id = 'test_enrich'

    delete_topics([in_topic, out_topic], bootstrap_server)
    delete_consumer_groups([group_id], bootstrap_server)

    kf = KafkaFaker(
        bootstrap_servers=bootstrap_server,
        num_messages=num_messages,
        topic=in_topic,
    )
    kf.publish()

    conf = new_from_path(
        path=os.path.join(settings.CONF_DIR, 'examples', 'enrich.yml'),
        setting_overrides={
            'kafka_brokers': bootstrap_server,
        },
    )

    stats = start(conf, max_msgs=num_messages)
    assert stats.num_messages_consumed == num_messages
    print(stats)

    messages = read_all_kafka_messages(bootstrap_server, out_topic)
    assert len(messages) == 1000, f"Expected 1000 messages, but got {len(messages)}"


@unittest.skip
def test_mem_persistence_window_tumbling(bootstrap_server):
    num_messages = 500000
    topic = 'mem-persistence-tumbling-window'
    admin_client = AdminClient({'bootstrap.servers': bootstrap_server})
    fs = admin_client.delete_topics([topic], operation_timeout=30)
    for f in fs.values():
        try:
            f.result()
        except KafkaException:
            pass

    kf = KafkaFaker(
        bootstrap_servers=bootstrap_server,
        num_messages=num_messages,
        topic=topic,
    )
    kf.publish()

    # run sql flow providing the kafka bootstrap server
    conf = new_from_path(
        path=os.path.join(settings.CONF_DIR, 'examples', 'tumbling.window.yml'),
        setting_overrides={
            'kafka_brokers': bootstrap_server,
            'topic': topic,
        },
    )

    stats = start(conf, max_msgs=num_messages)
    assert stats.num_messages_consumed == num_messages
    print(stats)

def test_dlq_functionality_handler_write(bootstrap_server):
    num_messages = 1
    in_topic = 'input-dlq-test'
    dlq_topic = 'dlq-dlq-test'
    group_id = 'test_dlq_functionality'

    # Clean up topics and consumer groups
    delete_topics([in_topic, dlq_topic], bootstrap_server)
    delete_consumer_groups([group_id], bootstrap_server)

    # Publish invalid JSON to the input topic
    conf = {
        'bootstrap.servers': bootstrap_server,
        'client.id': socket.gethostname()
    }
    producer = Producer(conf)
    producer.produce(in_topic, value=b'{!invalidJSON!')
    producer.flush()

    # Configure pipeline with DLQ enabled
    conf = new_from_path(
        path=os.path.join(settings.CONF_DIR, 'examples', 'kafka.dlq.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
            'SQLFLOW_INPUT_TOPIC': in_topic,
            'SQLFLOW_DLQ_TOPIC': dlq_topic,
            'SQLFLOW_SOURCE_ERROR_POLICY': 'dlq',
            'SQLFLOW_BATCH_SIZE': 1,
        },
    )

    # Run the pipeline
    stats = start(conf, max_msgs=num_messages)
    assert stats.num_messages_consumed == 1
    assert stats.num_errors == 1

    # Verify DLQ topic contains the error message
    dlq_messages = read_all_kafka_messages(bootstrap_server, dlq_topic)
    assert len(dlq_messages) == 1, f"Expected 1 DLQ message, but got {len(dlq_messages)}"
    m = dlq_messages[0]
    assert m['error'] == 'Expecting property name enclosed in double quotes: line 1 column 2 (char 1)'
    assert m['message'] == '{!invalidJSON!'
    assert m['phase'] == 'handler.write'

def test_dlq_functionality_handler_invoke(bootstrap_server):
    num_messages = 1
    in_topic = 'input-dlq-test'
    dlq_topic = 'dlq-dlq-test'
    group_id = 'test_dlq_functionality'

    # Clean up topics and consumer groups
    delete_topics([in_topic, dlq_topic], bootstrap_server)
    delete_consumer_groups([group_id], bootstrap_server)

    # Publish invalid JSON to the input topic
    conf = {
        'bootstrap.servers': bootstrap_server,
        'client.id': socket.gethostname()
    }
    producer = Producer(conf)
    producer.produce(in_topic, value=b'{"valid": "json"}')
    producer.flush()

    # Configure pipeline with DLQ enabled
    conf = new_from_path(
        path=os.path.join(settings.CONF_DIR, 'examples', 'kafka.dlq.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
            'SQLFLOW_INPUT_TOPIC': in_topic,
            'SQLFLOW_DLQ_TOPIC': dlq_topic,
            'SQLFLOW_SOURCE_ERROR_POLICY': 'dlq',
            'SQLFLOW_BATCH_SIZE': 1,
        },
    )

    # Run the pipeline
    stats = start(conf, max_msgs=num_messages)
    assert stats.num_messages_consumed == 1
    assert stats.num_errors == 1

    # Verify DLQ topic contains the error message
    dlq_messages = read_all_kafka_messages(bootstrap_server, dlq_topic)
    assert len(dlq_messages) == 1, f"Expected 1 DLQ message, but got {len(dlq_messages)}"
    m = dlq_messages[0]
    assert m['error'] == 'Binder Error: Referenced column "broken" not found in FROM clause!\nCandidate bindings: "valid"\n\nLINE 2:   broken\n          ^'
    assert m['message'] == 'Handler invocation failed'
    assert m['phase'] == 'handler.invoke'
