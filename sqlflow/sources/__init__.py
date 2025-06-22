from confluent_kafka import Consumer

from sqlflow import config

from .kafka import KafkaSource
from .websocket import WebsocketSource
from .webhook import WebhookSource, HMACConfig


def new_consumer_from_conf(conf: config.KafkaSource):
    kconf = {
        'bootstrap.servers': ','.join(conf.brokers),
        'group.id': conf.group_id,
        'auto.offset.reset': conf.auto_offset_reset,
        'enable.auto.commit': False,
    }

    if conf.security_protocol:
        kconf['security.protocol'] = conf.security_protocol

    if conf.sasl:
        kconf['sasl.mechanism'] = conf.sasl.mechanism
        kconf['sasl.username'] = conf.sasl.username
        kconf['sasl.password'] = conf.sasl.password

    if conf.ssl:
        kconf['ssl.ca.location'] = conf.ssl.ca_location
        kconf['ssl.certificate.location'] = conf.ssl.certificate_location
        kconf['ssl.key.location'] = conf.ssl.key_location
        kconf['ssl.key.password'] = conf.ssl.key_password
        kconf['ssl.endpoint.identification.algorithm'] = conf.ssl.endpoint_identification_algorithm

    consumer = Consumer(kconf)
    return consumer


def new_source_from_conf(source_conf: config.Source):
    if source_conf.type == 'kafka':
        consumer = new_consumer_from_conf(source_conf.kafka)

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

