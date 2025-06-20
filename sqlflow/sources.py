import logging
import typing
from abc import ABC, abstractmethod
from typing import Iterator

from fastapi import FastAPI, Request
from threading import Thread, Lock
from queue import Queue, Empty
import uvicorn

from websockets.sync.client import connect

from confluent_kafka import KafkaError, Consumer

from sqlflow import config

logger = logging.getLogger(__name__)


class Message:
    def __init__(self, value: bytes):
        self._value = value

    def value(self) -> bytes:
        return self._value


class Source(ABC):
    """
    Abstract base class for sources.

    Sources read bytes from some external source and return them as messages.
    """
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def stream(self) -> Iterator[Message | None]:
        pass

    @abstractmethod
    def start(self):
        pass


class SourceException(Exception):
    pass


class KafkaSource(Source):
    def __init__(self,
                 consumer,
                 topics,
                 async_commit=False,
                 read_timeout=5.0):
        self._consumer = consumer
        self._async_commit = async_commit
        self._read_timeout = read_timeout
        self._topics = topics

    def close(self):
        return self._consumer.close()

    def commit(self):
        return self._consumer.commit(
            asynchronous=self._async_commit,
        )

    def stream(self) -> typing.Iterable[Message | None]:
        while True:
            msg = self._consumer.poll(timeout=self._read_timeout)

            if msg is None:
                yield None
            elif not msg.error():
                yield Message(msg.value())
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                logger.warning(
                    '%% %s [%d] reached end at offset %d'.format(
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )
                )
                yield None
            else:
                # unknown error raise to caller
                raise SourceException(msg.error())

    def start(self):
        self._consumer.subscribe(self._topics)


class WebsocketSource(Source):
    def __init__(self, uri):
        self._uri = uri

    def start(self):
        pass

    def commit(self):
        pass

    def close(self):
        pass

    def stream(self) -> Message | None:
        logger.info("connecting to websocket: {}".format(self._uri))
        with connect(self._uri) as websocket:
            while True:
                for msg in websocket.recv_streaming(decode=False):
                    yield Message(msg)


class FastAPISource(Source):
    def __init__(self, host="0.0.0.0", port=8001):
        self._app = FastAPI()
        self._queue = Queue()
        self._lock = Lock()
        self._host = host
        self._port = port
        self._server_thread = None

        # Define the FastAPI endpoint
        @self._app.post("/events")
        async def receive_events(request: Request):
            data = await request.body()
            with self._lock:
                self._queue.put(Message(data))
            return {"status": "received"}

    def start(self):
        # Start the FastAPI server in a separate thread
        logger.info('Starting FastAPI server at http://{}:{}'.format(self._host, self._port))
        def run_server():
            uvicorn.run(
                self._app,
                host=self._host,
                port=self._port,
                log_level="info",
            )

        self._server_thread = Thread(target=run_server, daemon=True)
        self._server_thread.start()

    def stream(self):
        # Return an iterator over the messages in the queue
        while True:
            try:
                yield self._queue.get(timeout=1)
            except Empty:
                yield None

    def commit(self):
        # Commit logic (if needed) can be implemented here
        pass

    def close(self):
        # Close the source (if needed)
        pass


def new_source_from_conf(source_conf: config.Source):
    if source_conf.type == 'kafka':
        kconf = {
            'bootstrap.servers': ','.join(source_conf.kafka.brokers),
            'group.id': source_conf.kafka.group_id,
            'auto.offset.reset': source_conf.kafka.auto_offset_reset,
            'enable.auto.commit': False,
        }

        consumer = Consumer(kconf)

        return KafkaSource(
            consumer=consumer,
            topics=source_conf.kafka.topics,
        )
    elif source_conf.type == 'websocket':
        return WebsocketSource(
            uri=source_conf.websocket.uri,
        )
    elif source_conf.type == 'webhook':
        return FastAPISource()

    raise NotImplementedError('unsupported source type: {}'.format(source_conf.type))

