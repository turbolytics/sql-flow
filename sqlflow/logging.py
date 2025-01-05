import logging

from sqlflow import settings


def init():
    logging.getLogger('websockets').setLevel(logging.WARN)
    logging.basicConfig(
        level=settings.LOG_LEVEL,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )