import copy
import random

import click
import json

from confluent_kafka import Producer
import socket

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


@click.command()
@click.option('--num-messages', default=1001, type=int)
@click.option('--topic', default='topic-1')
def main(num_messages, topic):
    conf = {
      'bootstrap.servers': 'localhost:9092',
      'client.id': socket.gethostname()
    }

    producer = Producer(conf)
    for i in range(num_messages):
        e = copy.deepcopy(event)
        e['properties']['city'] = random.choice(cities)
        j_event = json.dumps(e)
        producer.produce(topic, value=j_event)
        if i % 1000 == 0:
            print('published {} of {}'.format(i, num_messages))
            producer.flush()
    producer.flush()


if __name__ == '__main__':
    main()