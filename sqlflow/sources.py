import asyncio
import functools
import logging
import typing
from abc import ABC, abstractmethod

import websockets
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
    async def close(self):
        pass

    @abstractmethod
    async def commit(self):
        pass

    @abstractmethod
    async def stream(self) -> typing.AsyncIterator[Message | None]:
        pass

    @abstractmethod
    async def start(self):
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

    async def close(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._consumer.close)

    async def commit(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
                None,
                functools.partial(
                    self._consumer.commit,
                    asynchronous=self._async_commit,
                )
        )

    async def stream(self) -> typing.AsyncIterator[Message | None]:
        loop = asyncio.get_event_loop()
        while True:
            msg = await loop.run_in_executor(None, self._consumer.poll, self._read_timeout)
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
                raise SourceException(msg.error())

    async def start(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._consumer.subscribe, self._topics)


class WebsocketSource(Source):
    def __init__(self, uri):
        self._uri = uri

    async def start(self):
        pass

    async def commit(self):
        pass

    async def close(self):
        pass

    async def stream(self) -> typing.AsyncIterator[Message | None]:
        logger.info("connecting to websocket: {}".format(self._uri))
        async with websockets.connect(self._uri) as websocket:
            while True:
                msg = await websocket.recv(decode=False)
                yield Message(msg)
