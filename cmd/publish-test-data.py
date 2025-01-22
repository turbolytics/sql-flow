import click

from sqlflow.fixtures import KafkaFaker
from sqlflow import logging


logging.init()


@click.command()
@click.option('--num-messages', default=1001, type=int)
@click.option('--topic', default='topic-1')
@click.option('--fixture', default=None)
def main(num_messages, topic, fixture):
    kf = KafkaFaker(
        bootstrap_servers='localhost:9092',
        num_messages=num_messages,
        topic=topic,
        fixture=fixture,
    )

    kf.publish()


if __name__ == '__main__':
    main()