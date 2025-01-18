import json

from confluent_kafka import KafkaException, Consumer, KafkaError
from confluent_kafka.admin import AdminClient


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
        'group.id': 'test_group',
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
