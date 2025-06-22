import logging
import typing

from confluent_kafka import KafkaError

from .base import Source, Message, SourceException


logger = logging.getLogger(__name__)


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
