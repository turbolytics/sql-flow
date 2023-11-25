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


def main():
    conf = {
      'bootstrap.servers': 'localhost:9092',
      'client.id': socket.gethostname()
    }
    j_event = json.dumps(event)

    producer = Producer(conf)
    i = 0
    while True:
        producer.produce('topic-1', value=j_event)
        i += 1
        if i % 1000 == 0:
            print('here')
            producer.flush()
    producer.flush()


if __name__ == '__main__':
    main()