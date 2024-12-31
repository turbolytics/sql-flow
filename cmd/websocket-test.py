from datetime import datetime, timezone

from websockets.sync.client import connect

from sqlflow import logging


logging.init()

def main():
    num_messages = 0
    uri = 'wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post'
    start = datetime.now(timezone.utc)
    with connect(uri) as websocket:
        for msg in websocket:
            num_messages += 1
            # print(f"Received message {num_messages}: {msg.value()}")

            if num_messages % 1000 == 0:
                now = datetime.now(timezone.utc)
                diff = (now - start)
                print(f'{num_messages} messages received in {diff.total_seconds()} seconds = {num_messages // diff.total_seconds()} messages / second')


if __name__ == '__main__':
    main()
