import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import socket

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from sqlflow.managers import window
from sqlflow.outputs import ConsoleWriter, Writer, KafkaWriter


logger = logging.getLogger(__name__)


@dataclass
class Stats:
    start_time: datetime = datetime.now(timezone.utc)
    num_messages_consumed: int = 0
    num_errors: int = 0
    total_throughput_per_second: float = 0


class SQLFlow:
    '''
    SQLFlow executes a pipeline as a daemon.
    '''

    def __init__(self, input, consumer, handler, output: Writer):
        self.input = input
        self.consumer = consumer
        self.output = output
        self.handler = handler
        self._stats = Stats(
            num_messages_consumed=0,
            num_errors=0,
            start_time=datetime.now(timezone.utc),
        )

    def consume_loop(self, max_msgs=None):
        logger.info('consumer loop starting')
        try:
            self.consumer.subscribe(self.input.kafka.topics)
            self._consume_loop(max_msgs)

            now = datetime.now(timezone.utc)
            diff = (now - self._stats.start_time)
            self._stats.total_throughput_per_second = self._stats.num_messages_consumed // diff.total_seconds()
        except Exception as e:
            logger.error('error in consumer loop: {}'.format(e))
            raise e
        finally:
            self.consumer.close()
            logger.info(
                'consumer loop ending: total messages / sec = {}'.format(self._stats.total_throughput_per_second),
            )
            return self._stats


    def _consume_loop(self, max_msgs=None):
        num_batch_messages = 0
        self._stats.start_time = datetime.now(timezone.utc)
        self._stats.num_messages_consumed = 0

        self.handler.init()

        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            self._stats.num_messages_consumed += 1
            if msg.error():
                self._stats.num_errors += 1
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write(
                        '%% %s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset()),
                        )
                elif msg.error():
                    raise KafkaException(msg.error())
                continue

            self.handler.write(msg.value().decode())
            num_batch_messages += 1

            if self._stats.num_messages_consumed % 10000 == 0:
                now = datetime.now(timezone.utc)
                diff = (now - self._stats.start_time)
                logger.debug('{}: reqs / second'.format(
                    self._stats.num_messages_consumed // diff.total_seconds()),
                )

            if num_batch_messages == self.input.batch_size:
                # apply the pipeline
                batch = self.handler.invoke()
                for l in batch:
                    self.output.write(l)

                # Only commit after all messages in batch are processed
                self.output.flush()
                self.consumer.commit(asynchronous=False)

                # reset the file state
                self.handler.init()
                num_batch_messages = 0

            if max_msgs and max_msgs <= self._stats.num_messages_consumed:
                logger.info('max messages reached')
                return


def init_tables(conn, tables):
    for csv_table in tables.csv:
        stmnt = "CREATE TABLE {} AS SELECT * from read_csv('{}', header={}, auto_detect={})".format(
            csv_table.name,
            csv_table.path,
            csv_table.header,
            csv_table.auto_detect
        )
        conn.sql(stmnt)

    for sql_table in tables.sql:
        conn.sql(sql_table.sql)


def build_managed_tables(conn, kafka_conf, table_confs):
    managed_tables = []
    for table in table_confs:
        # windowed tables are the only supported tables currently
        if not table.manager:
            continue

        if not table.manager.tumbling_window:
            raise NotImplementedError('only tumbling_window manager currently supported')

        output = ConsoleWriter()
        if table.manager.output.type == 'kafka':
            output = new_kafka_output_from_conf(
                brokers=kafka_conf.brokers,
                topic=table.manager.output.topic,
            )

        h = window.Tumbling(
            conn=conn,
            table=window.Table(
                name=table.name,
                time_field=table.manager.tumbling_window.time_field,
            ),
            size_seconds=table.manager.tumbling_window.duration_seconds,
            writer=output,
        )
        managed_tables.append(h)
    return managed_tables


def handle_managed_tables(tables):
    """
    Starts table management routines. Some tables are managed throughout the
    lifetime of the sqlflow process, such as windowed tables.

    This routine kicks off that management.

    :param conn:
    :param tables:
    :return:
    """
    for handler in tables:
        t = threading.Thread(
            target=handler.start,
        )
        t.start()


def new_kafka_output_from_conf(brokers, topic):
    p = Producer({
        'bootstrap.servers': ','.join(brokers),
        'client.id': socket.gethostname(),
    })
    return KafkaWriter(
        topic=topic,
        producer=p,
    )

def new_sqlflow_from_conf(conf, conn, handler) -> SQLFlow:
    kconf = {
        'bootstrap.servers': ','.join(conf.pipeline.source.kafka.brokers),
        'group.id': conf.pipeline.source.kafka.group_id,
        'auto.offset.reset': conf.pipeline.source.kafka.auto_offset_reset,
        'enable.auto.commit': False,
    }

    consumer = Consumer(kconf)

    output = ConsoleWriter()
    if conf.pipeline.sink.type == 'kafka':
        output = new_kafka_output_from_conf(
            brokers=conf.pipeline.source.kafka.brokers,
            topic=conf.pipeline.sink.kafka.topic,
        )

    sflow = SQLFlow(
        input=conf.pipeline.source,
        consumer=consumer,
        handler=handler,
        output=output,
    )

    return sflow

