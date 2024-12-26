import click

from sqlflow.fixtures import KafkaFaker
from sqlflow import logging


logging.init()


@click.command()
@click.option('--num-messages', default=1001, type=int)
@click.option('--topic', default='topic-1')
def main(num_messages, topic):
    kf = KafkaFaker(
        bootstrap_servers='localhost:9092',
        num_messages=num_messages,
        topic=topic
    )

    kf.publish()


if __name__ == '__main__':
    main()