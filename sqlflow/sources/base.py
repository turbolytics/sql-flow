import logging
from abc import ABC, abstractmethod
from typing import Iterator


logger = logging.getLogger(__name__)

class Message:
    def __init__(self, value: bytes, topic: str | None = None, partition: int | None = None, offset: int | None = None):
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str | None:
        return self._topic

    def partition(self) -> int | None:
        return self._partition

    def offset(self) -> int | None:
        return self._offset

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





