import sys
from abc import abstractmethod, ABC


class Sink(ABC):
    @abstractmethod
    def write(self, val: bytes, key: bytes = None):
        """
        Writes a byte string to the underlying storage.

        :param val:
        :param key:
        :return:
        """
        raise NotImplemented()

    @abstractmethod
    def flush(self):
        """
        Flushes any buffered data to the underlying storage.

        :return:
        """
        pass


class TestSink(Sink):
    def __init__(self):
        self.writes = []

    def write(self, val: bytes, key: bytes = None):
        self.writes.append((key, val))

    def flush(self):
        pass


class ConsoleSink(Sink):
    def write(self, val: bytes, key: bytes = None):
        sys.stdout.write(val)
        sys.stdout.write('\n')

    def flush(self):
        pass


class KafkaSink(Sink):
    def __init__(self, topic, producer):
        self.topic = topic
        self.producer = producer

    def write(self, val: bytes, key: bytes = None):
        self.producer.produce(self.topic, key=key, value=val.encode('utf-8'))

    def flush(self):
        self.producer.flush()
