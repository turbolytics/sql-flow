import duckdb
import sys
import threading
from datetime import datetime, timezone
import logging
import socket

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from sqlflow import window
from sqlflow.outputs import ConsoleWriter, Writer, KafkaWriter


logger = logging.getLogger(__name__)


class SQLFlow:
    '''
    SQLFlow executes a pipeline as a daemon.
    '''

    def __init__(self, input, consumer, handler, output: Writer):
        self.input = input
        self.consumer = consumer
        self.output = output
        self.handler = handler

    def consume_loop(self, max_msgs=None):
        logger.info('consumer loop starting')
        try:
            self.consumer.subscribe(self.input.topics)
            self._consume_loop(max_msgs)
        finally:
            self.consumer.close()

    def _consume_loop(self, max_msgs=None):
        num_messages = 0
        start_dt = datetime.now(timezone.utc)
        total_messages = 0

        self.handler.init()

        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            total_messages += 1
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        '%% %s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset()),
                        )
                elif msg.error():
                    raise KafkaException(msg.error())
                continue

            self.handler.write(msg.value().decode())
            num_messages += 1

            if total_messages % 10000 == 0:
                now = datetime.now(timezone.utc)
                diff = (now - start_dt)
                logger.debug('{}: reqs / second'.format(total_messages // diff.total_seconds()))

            if num_messages == self.input.batch_size:
                # apply the pipeline
                batch = self.handler.invoke()
                for l in batch:
                    self.output.write(l)

                # Only commit after all messages in batch are processed
                self.output.flush()
                self.consumer.commit(asynchronous=False)

                # reset the file state
                self.handler.init()
                num_messages = 0

            if max_msgs and max_msgs <= total_messages:
                now = datetime.now(timezone.utc)
                diff = (now - start_dt)
                logger.debug('{}: reqs / second - Total'.format(total_messages // diff.total_seconds()))
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


def build_managed_tables(conn, table_confs):
    managed_tables = []
    for table in table_confs:
        # windowed tables are the only supported tables currently
        if not table.window:
            continue

        if table.window.type != 'tumbling':
            raise NotImplementedError('only tumbling window is supported')

        h = window.Tumbling(
            conn=conn,
            table=window.Table(
                name=table.name,
                time_field=table.window.time_field,
            ),
            size_seconds=table.window.duration_seconds,
            writer=ConsoleWriter(),
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


def new_sqlflow_from_conf(conf, conn, handler) -> SQLFlow:
    kconf = {
        'bootstrap.servers': ','.join(conf.kafka.brokers),
        'group.id': conf.kafka.group_id,
        'auto.offset.reset': conf.kafka.auto_offset_reset,
        'enable.auto.commit': False,
    }

    consumer = Consumer(kconf)

    output = ConsoleWriter()
    if conf.pipeline.output.type == 'kafka':
        producer = Producer({
            'bootstrap.servers': ','.join(conf.kafka.brokers),
            'client.id': socket.gethostname(),
        })
        output = KafkaWriter(
            topic=conf.pipeline.output.topic,
            producer=producer,
        )

    sflow = SQLFlow(
        input=conf.pipeline.input,
        consumer=consumer,
        handler=handler,
        output=output,
    )

    return sflow

