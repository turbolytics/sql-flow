import click

from sqlflow.fixtures import KafkaFaker
from sqlflow import logging


logging.init()


@click.command()
@click.option('--num-messages', default=1001, type=int)
@click.option('--topic', default='topic-1')
@click.option('--fixture', default=None)
@click.option('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
@click.option('--security-protocol', default=None, help='Kafka security protocol')
@click.option('--sasl-mechanism', default=None, help='SASL mechanism')
@click.option('--sasl-username', default=None, help='SASL username')
@click.option('--sasl-password', default=None, help='SASL password')
@click.option('--ssl-ca-location', default=None, help='Path to CA certificate')
@click.option('--ssl-key-location', default=None, help='Path to client key')
@click.option('--ssl-certificate-location', default=None, help='Path to client certificate')
@click.option('--ssl-key-password', default=None, help='Password for client key')
@click.option('--ssl-endpoint-identification-algorithm', default='none', help='SSL endpoint identification algorithm')
def main(num_messages, topic, fixture, bootstrap_servers, security_protocol, sasl_mechanism, sasl_username, sasl_password, ssl_ca_location, ssl_key_location, ssl_certificate_location, ssl_key_password, ssl_endpoint_identification_algorithm):
    kf = KafkaFaker(
        bootstrap_servers=bootstrap_servers,
        num_messages=num_messages,
        topic=topic,
        fixture=fixture,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
        ssl_ca_location=ssl_ca_location,
        ssl_key_location=ssl_key_location,
        ssl_certificate_location=ssl_certificate_location,
        ssl_key_password=ssl_key_password,
        ssl_endpoint_identification_algorithm=ssl_endpoint_identification_algorithm
    )

    kf.publish()


if __name__ == '__main__':
    main()