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
    def __init__(self,
                 bootstrap_servers,
                 num_messages,
                 topic,
                 fixture=None,
                 security_protocol=None,
                 sasl_mechanism=None,
                 sasl_username=None,
                 sasl_password=None,
                 ssl_ca_location=None,
                 ssl_key_location=None,
                 ssl_certificate_location=None,
                 ssl_key_password=None,
                 ssl_endpoint_identification_algorithm=None
                ):
        self.bootstrap_servers = bootstrap_servers
        self.num_messages = num_messages
        self.topic = topic
        self.fixture = fixture
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.ssl_ca_location = ssl_ca_location
        self.ssl_key_location = ssl_key_location
        self.ssl_certificate_location = ssl_certificate_location
        self.ssl_key_password = ssl_key_password
        self.ssl_endpoint_identification_algorithm = ssl_endpoint_identification_algorithm

    def publish(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': socket.gethostname()
        }

        if self.security_protocol:
            conf['security.protocol'] = self.security_protocol
        if self.sasl_mechanism:
            conf['sasl.mechanism'] = self.sasl_mechanism
        if self.sasl_username and self.sasl_password:
            conf['sasl.username'] = self.sasl_username
            conf['sasl.password'] = self.sasl_password
        if self.ssl_ca_location:
            conf['ssl.ca.location'] = self.ssl_ca_location
        if self.ssl_key_location:
            conf['ssl.key.location'] = self.ssl_key_location
        if self.ssl_certificate_location:
            conf['ssl.certificate.location'] = self.ssl_certificate_location
        if self.ssl_key_password:
            conf['ssl.key.password'] = self.ssl_key_password
        if self.ssl_endpoint_identification_algorithm:
            conf['ssl.endpoint.identification.algorithm'] = self.ssl_endpoint_identification_algorithm

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


