import logging
import copy
import json
import socket
import random

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


event = {
    "anonymousId": "23adfd82-aa0f-45a7-a756-24f2a7a4c895",
    "context": {
        "library": {
            "name": "analytics.js",
            "version": "2.11.1"
        },
        "ip": "108.0.78.21"
    },
    "event": "search_event",
    "properties": {
        "city": "San Francisco",
        "country": "USA",
        "pickup_date": "2018-12-12T19:11:01.266Z",
        "drop_off_date": "2018-12-13T17:00:00.266Z",
        "business_class": True,
    },
    "receivedAt": "2015-12-12T19:11:01.266Z",
    "sentAt": "2015-12-12T19:11:01.169Z",
    "timestamp": "2015-12-12T19:11:01.249Z",
    "type": "track",
    "userId": "AiUGstSDIg",
    "originalTimestamp": "2015-12-12T19:11:01.152Z"
}

cities = [
    'San Fransisco',
    'Baltimore',
    'New York',
    'Miami',
    'Asheville',
]


class KafkaFaker:
    def __init__(self, bootstrap_servers, num_messages, topic):
        self.bootstrap_servers = bootstrap_servers
        self.num_messages = num_messages
        self.topic = topic

    def publish(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': socket.gethostname()
        }

        producer = Producer(conf)
        for i in range(self.num_messages):
            e = copy.deepcopy(event)
            e['properties']['city'] = random.choice(cities)
            j_event = json.dumps(e)
            producer.produce(self.topic, value=j_event)
            if i % 10000 == 0:
                logger.info('published {} of {}'.format(i, self.num_messages))
                producer.flush()
        producer.flush()
        logger.info('published {} of {}'.format(self.num_messages, self.num_messages))


