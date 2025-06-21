import logging
from abc import ABC, abstractmethod
from typing import Iterator


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





