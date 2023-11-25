import sys
from abc import abstractmethod, ABC


class Writer(ABC):
    @abstractmethod
    def write(self, bs: bytes):
        """
        Writes a byte string to the underlying storage.

        :param bs:
        :return:
        """
        raise NotImplemented()


class ConsoleWriter(Writer):
    def write(self, bs: bytes):
        sys.stdout.write(bs)
        sys.stdout.write('\n')


class KafkaWriter(Writer):
    def __init__(self):
        pass

    def write(self, bs: bytes):
        pass