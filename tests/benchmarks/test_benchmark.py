import os

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from testcontainers.kafka import KafkaContainer

from sqlflow.config import new_from_path
from sqlflow.fixtures import KafkaFaker
from sqlflow import settings
from sqlflow.lifecycle import start


@pytest.fixture(scope="module")
def bootstrap_server():
    # Start the Kafka container
    with KafkaContainer() as kafka:
        yield kafka.get_bootstrap_server()


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

    # publish 100k records to kafka
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
