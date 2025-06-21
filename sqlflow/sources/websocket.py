import logging

from websockets.sync.client import connect
from .base import Source, Message


logger = logging.getLogger(__name__)


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
