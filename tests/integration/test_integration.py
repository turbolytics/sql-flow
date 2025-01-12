import json
import os
import unittest

import pytest
from confluent_kafka import KafkaException, Consumer, KafkaError
from confluent_kafka.admin import AdminClient
from testcontainers.kafka import KafkaContainer

from sqlflow.config import new_from_path
from sqlflow.fixtures import KafkaFaker
from sqlflow import settings
from sqlflow.lifecycle import start



def delete_topics(topics, bootstrap_server):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_server})
    fs = admin_client.delete_topics(
        topics=topics,
        operation_timeout=30,
    )
    for f in fs.values():
        try:
            f.result()
        except KafkaException:
            pass

def delete_consumer_groups(consumer_groups, bootstrap_server):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_server})
    fs = admin_client.delete_consumer_groups(
        consumer_groups,
    )
    for f in fs.values():
        try:
            f.result()
        except KafkaException:
            pass

def read_all_kafka_messages(bootstrap_server, topic):
    kconf = {
        'bootstrap.servers': bootstrap_server,
        'group.id': 'test_basic_agg_mem',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }

    consumer = Consumer(kconf)
    consumer.subscribe([topic])

    # read until end of kafka
    messages = []
    while True:
        msg = consumer.poll(timeout=1)
        if msg is None:
            break
        elif not msg.error():
            messages.append(
                json.loads(
                    msg.value()
                )
            )
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            break
        else:
            raise Exception()

    consumer.close()

    return messages

@pytest.fixture(scope="module")
def bootstrap_server():
    # Start the Kafka container
    with KafkaContainer() as kafka:
        yield kafka.get_bootstrap_server()


def test_basic_agg_mem(bootstrap_server):
    num_messages = 1000
    in_topic = 'topic-simple-agg-mem'
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
            'kafka_brokers': bootstrap_server,
        },
    )

    stats = start(conf, max_msgs=num_messages)
    assert stats.num_messages_consumed == num_messages
    print(stats)

    messages = read_all_kafka_messages(bootstrap_server, out_topic)
    total_city_count = sum([m['city_count'] for m in messages])
    assert total_city_count == 1000, f"Expected city_count sum to be 1000, but got {total_city_count}"


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
