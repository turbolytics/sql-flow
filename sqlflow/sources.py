import logging
import typing
from abc import ABC, abstractmethod
from typing import Iterator

from websockets.sync.client import connect

from confluent_kafka import KafkaError

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
