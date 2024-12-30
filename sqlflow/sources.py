import logging
from abc import ABC, abstractmethod, abstractproperty
from xml.sax.handler import property_dom_node

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
    def read(self) -> Message | None:
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
                 read_timeout=1.0):
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

    def read(self) -> Message | None:
        msg = self._consumer.poll(timeout=self._read_timeout)

        if msg is None:
            return None

        if not msg.error():
            return Message(msg.value())

        if msg.error().code() == KafkaError._PARTITION_EOF:
            logger.warning(
                '%% %s [%d] reached end at offset %d'.format(
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
            )
            return None

        # unknown error raise to caller
        raise SourceException(msg.error())

    def start(self):
        self._consumer.subscribe(self._topics)


