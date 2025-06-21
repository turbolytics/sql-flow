from confluent_kafka import Consumer

from sqlflow import config

from .kafka import KafkaSource
from .websocket import WebsocketSource
from .webhook import WebhookSource, HMACConfig


def new_source_from_conf(source_conf: config.Source):
    if source_conf.type == 'kafka':
        kconf = {
            'bootstrap.servers': ','.join(source_conf.kafka.brokers),
            'group.id': source_conf.kafka.group_id,
            'auto.offset.reset': source_conf.kafka.auto_offset_reset,
            'enable.auto.commit': False,
        }

        consumer = Consumer(kconf)

        return KafkaSource(
            consumer=consumer,
            topics=source_conf.kafka.topics,
        )
    elif source_conf.type == 'websocket':
        return WebsocketSource(
            uri=source_conf.websocket.uri,
        )
    elif source_conf.type == 'webhook':
        hmac_config = None
        if source_conf.webhook.signature_type == 'hmac' and source_conf.webhook.hmac:
            hmac_config = HMACConfig(
                header=source_conf.webhook.hmac.header,
                sig_key=source_conf.webhook.hmac.sig_key,
                secret=source_conf.webhook.hmac.secret,
            )
        return WebhookSource(hmac_config=hmac_config)

    raise NotImplementedError('unsupported source type: {}'.format(source_conf.type))

