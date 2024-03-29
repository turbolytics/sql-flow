import json
from abc import ABC, abstractmethod


class Serializer(ABC):
    @abstractmethod
    def encode(self, d: object) -> bytes:
        pass


class Deserializer(ABC):
    @abstractmethod
    def encode(self, bs: bytes) -> object:
        pass


class JSON(Serializer, Deserializer):
    def decode(self, bs: bytes) -> object:
        return json.loads(bs)

    def encode(self, d: object) -> bytes:
        return json.dumps(d)


