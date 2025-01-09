import json
from abc import ABC, abstractmethod
from datetime import datetime


class Serializer(ABC):
    @abstractmethod
    def encode(self, d: object) -> bytes:
        pass


class Deserializer(ABC):
    @abstractmethod
    def encode(self, bs: bytes) -> object:
        pass


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime)):
            # Convert datetime to ISO 8601 string
            return obj.isoformat()
        return super().default(obj)


class JSON(Serializer, Deserializer):
    def decode(self, bs: bytes) -> object:
        return json.loads(bs)

    def encode(self, d: object) -> bytes:
        return json.dumps(d, cls=DateTimeEncoder)


class Noop(Serializer, Deserializer):
    def decode(self, bs: bytes):
        raise NotImplemented

    def encode(self, d: object) -> object:
        return d

