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

from sqlflow.fixtures import KafkaFaker
from sqlflow import settings
from sqlflow.kafka import delete_topics, delete_consumer_groups, read_all_kafka_messages
from tests.integration.engines import (
    run_pipeline,
    assert_all_messages_accounted_for,
)


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

    # The Python engine builds this catalog in-process, but the Go engine runs
    # as a subprocess and resolves the catalog by name the way pyiceberg does.
    # Exporting it here keeps the test hermetic: without this it only passes on
    # a machine whose ~/.pyiceberg.yaml happens to define this catalog.
    env_prefix = f'PYICEBERG_CATALOG__{catalog_name.upper()}__'
    os.environ[env_prefix + 'URI'] = f"sqlite:///{warehouse_path}/catalog.db"
    os.environ[env_prefix + 'WAREHOUSE'] = f"file://{warehouse_path}"

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

    stats = run_pipeline(
        config_path=os.path.join(settings.CONF_DIR, 'examples', 'kafka.mem.iceberg.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
            'catalog_name': catalog_name,
            'table_name': table_name,
        },
        max_msgs=num_messages,
    )

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
        stats = run_pipeline(
            config_path=os.path.join(settings.CONF_DIR, 'examples', 'local.parquet.sink.yml'),
            setting_overrides={
                'kafka_brokers': bootstrap_server,
                'sink_base_path': temp_dir,
                'batch_size': 1000,
            },
            max_msgs=num_messages,
        )
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

    stats = run_pipeline(
        config_path=os.path.join(settings.CONF_DIR, 'examples', 'basic.agg.mem.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
        },
        max_msgs=num_messages,
    )
    assert stats.num_messages_consumed == num_messages
    print(stats)

    messages = read_all_kafka_messages(bootstrap_server, out_topic)
    assert 5 == len(messages)

    # The 5 rows are one per city; their counts must add back up to every
    # message published.
    assert_all_messages_accounted_for(
        stats, num_messages, messages, 'city_count')


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

    stats = run_pipeline(
        config_path=os.path.join(settings.CONF_DIR, 'examples', 'basic.agg.mem.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
            'SQLFLOW_SOURCE_ERROR_POLICY': 'ignore',
            'SQLFLOW_INPUT_TOPIC': in_topic,
            'SQLFLOW_OUTPUT_TOPIC': out_topic,
            'SQLFLOW_BATCH_SIZE': 1,
        },
        max_msgs=num_messages,
    )
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

    stats = run_pipeline(
        config_path=os.path.join(settings.CONF_DIR, 'examples', 'csv.mem.join.yml'),
        setting_overrides={
            'kafka_brokers': bootstrap_server,
            'STATIC_ROOT': settings.DEV_DIR,
        },
        max_msgs=num_messages,
    )
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

    stats = run_pipeline(
        config_path=os.path.join(settings.CONF_DIR, 'examples', 'enrich.yml'),
        setting_overrides={
            'kafka_brokers': bootstrap_server,
        },
        max_msgs=num_messages,
    )
    assert stats.num_messages_consumed == num_messages
    print(stats)

    messages = read_all_kafka_messages(bootstrap_server, out_topic)
    assert len(messages) == 1000, f"Expected 1000 messages, but got {len(messages)}"


# The Python engine's managed-table threads are never stopped
# (pipeline.handle_managed_tables starts them and nothing joins them), so this
# test leaves a manager polling and a consumer in group 'test', which starves
# the DLQ tests that share that group. Skipped for the Python engine; the Go
# engine stops its managers on shutdown and is covered end to end.
@unittest.skipIf(os.environ.get('SQLFLOW_ENGINE', 'python') == 'python',
                 'python manager threads outlive the test')
def test_mem_persistence_window_tumbling(bootstrap_server):
    num_messages = 2000
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
    stats = run_pipeline(
        config_path=os.path.join(settings.CONF_DIR, 'examples', 'tumbling.window.yml'),
        setting_overrides={
            'kafka_brokers': bootstrap_server,
            'topic': topic,
        },
        max_msgs=num_messages,
    )
    assert stats.num_messages_consumed == num_messages

    # The window manager publishes each closed window; between them they must
    # account for every message published.
    messages = read_all_kafka_messages(bootstrap_server, 'output-tumbling-window-1')
    assert_all_messages_accounted_for(stats, num_messages, messages, 'count')

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

    # Configure pipeline with DLQ enabled, then run it
    stats = run_pipeline(
        config_path=os.path.join(settings.CONF_DIR, 'examples', 'kafka.dlq.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
            'SQLFLOW_INPUT_TOPIC': in_topic,
            'SQLFLOW_DLQ_TOPIC': dlq_topic,
            'SQLFLOW_SOURCE_ERROR_POLICY': 'dlq',
            'SQLFLOW_BATCH_SIZE': 1,
        },
        max_msgs=num_messages,
    )
    assert stats.num_messages_consumed == 1
    assert stats.num_errors == 1

    # Verify DLQ topic contains the error message
    dlq_messages = read_all_kafka_messages(bootstrap_server, dlq_topic)
    assert len(dlq_messages) == 1, f"Expected 1 DLQ message, but got {len(dlq_messages)}"
    m = dlq_messages[0]
    # The wording of a JSON parse failure is engine-specific; that it is
    # reported, and against which message and phase, is not.
    assert m['error']
    assert m['message'] == '{!invalidJSON!'
    assert m['phase'] == 'handler.write'
    assert m['timestamp']

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

    # Configure pipeline with DLQ enabled, then run it
    stats = run_pipeline(
        config_path=os.path.join(settings.CONF_DIR, 'examples', 'kafka.dlq.yml'),
        setting_overrides={
            'SQLFLOW_KAFKA_BROKERS': bootstrap_server,
            'SQLFLOW_INPUT_TOPIC': in_topic,
            'SQLFLOW_DLQ_TOPIC': dlq_topic,
            'SQLFLOW_SOURCE_ERROR_POLICY': 'dlq',
            'SQLFLOW_BATCH_SIZE': 1,
        },
        max_msgs=num_messages,
    )
    assert stats.num_messages_consumed == 1
    assert stats.num_errors == 1

    # Verify DLQ topic contains the error message
    dlq_messages = read_all_kafka_messages(bootstrap_server, dlq_topic)
    assert len(dlq_messages) == 1, f"Expected 1 DLQ message, but got {len(dlq_messages)}"
    m = dlq_messages[0]
    assert 'Binder Error: Referenced column "broken" not found in FROM clause!' in m['error']
    assert m['message'] == 'Handler invocation failed'
    assert m['phase'] == 'handler.invoke'
