import logging
import copy
import json
import socket
import random
from datetime import datetime, timezone

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


event = {
    "ip": "103.22.200.1",
    "event": "search_event",
    "properties": {
        "city": "San Francisco",
        "country": "USA",
    },
    "timestamp": "2015-12-12T19:11:01.249Z",
    "type": "track",
    "userId": "AiUGstSDIg",
}

cities = [
    'San Fransisco',
    'Baltimore',
    'New York',
    'Miami',
    'Asheville',
]


class KafkaFaker:
    def __init__(self, bootstrap_servers, num_messages, topic, fixture=None):
        self.bootstrap_servers = bootstrap_servers
        self.num_messages = num_messages
        self.topic = topic
        self.fixture = fixture

    def publish(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': socket.gethostname()
        }

        producer = Producer(conf)
        for i in range(self.num_messages):
            if self.fixture is None:
                e = copy.deepcopy(event)
                e['properties']['city'] = random.choice(cities)
                e['timestamp'] = datetime.now(tz=timezone.utc).isoformat()
                j_event = json.dumps(e)
            elif self.fixture == 'user_action':
                e = {
                    "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                    "user_id": random.randint(1, 1000),
                    "action": random.choice(["button_click", "view", "scroll"]),
                    "browser": random.choice(["Chrome", "Firefox", "Safari", "IE", "Edge"]),
                }
                j_event = json.dumps(e)
            else:
                raise NotImplementedError('fixture {} not implemented'.format(self.fixture))
            producer.produce(self.topic, value=j_event)
            if i % 10000 == 0:
                logger.info('published {} of {}'.format(i, self.num_messages))
                producer.flush()
        producer.flush()
        logger.info('published {} of {}'.format(self.num_messages, self.num_messages))


