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
        import ipdb; ipdb.set_trace();

        if source_conf.kafka.security_protocol == 'SASL_PLAINTEXT':
            kconf['security.protocol'] = 'SASL_PLAINTEXT'
            kconf['sasl.mechanism'] = source_conf.kafka.sasl.mechanism
            kconf['sasl.username'] = source_conf.kafka.sasl.username
            kconf['sasl.password'] = source_conf.kafka.sasl.password

        if source_conf.kafka.security_protocol == 'SSL':
            kconf['security.protocol'] = 'SSL'
            kconf['ssl.ca.location'] = source_conf.kafka.ssl.ca_location
            kconf['ssl.certificate.location'] = source_conf.kafka.ssl.certificate_location
            kconf['ssl.key.location'] = source_conf.kafka.ssl.key_location
            kconf['ssl.key.password'] = source_conf.kafka.ssl.key_password
            kconf['ssl.endpoint.identification.algorithm'] = source_conf.kafka.ssl.endpoint_identification_algorithm

        if source_conf.kafka.security_protocol == 'SASL_SSL':
            kconf['security.protocol'] = 'SASL_SSL'
            kconf['sasl.mechanism'] = source_conf.kafka.sasl.mechanism
            kconf['sasl.username'] = source_conf.kafka.sasl.username
            kconf['sasl.password'] = source_conf.kafka.sasl.password
            kconf['ssl.ca.location'] = source_conf.kafka.ssl.ca_location
            kconf['ssl.certificate.location'] = source_conf.kafka.ssl.certificate_location
            kconf['ssl.key.location'] = source_conf.kafka.ssl.key_location
            kconf['ssl.key.password'] = source_conf.kafka.ssl.key_password
            kconf['ssl.endpoint.identification.algorithm'] = source_conf.kafka.ssl.endpoint_identification_algorithm

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

