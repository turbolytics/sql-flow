import threading
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import socket

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from sqlflow import config
from sqlflow.managers import window
from sqlflow.sinks import ConsoleSink, Sink, KafkaSink
from sqlflow.sources import Source, KafkaSource, WebsocketSource

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

    def __init__(self,
                 source: Source,
                 handler,
                 sink: Sink,
                 batch_size=1000):
        self.source = source
        self.sink = sink
        self.handler = handler
        self._batch_size = batch_size
        self._stats = Stats(
            num_messages_consumed=0,
            num_errors=0,
            start_time=datetime.now(timezone.utc),
        )

    def consume_loop(self, max_msgs=None):
        logger.info('consumer loop starting')
        try:
            self.source.start()
            self._consume_loop(max_msgs)

        except Exception as e:
            logger.error('error in consumer loop: {}'.format(e))
            raise e
        finally:
            self.source.close()

            now = datetime.now(timezone.utc)
            diff = (now - self._stats.start_time)
            self._stats.total_throughput_per_second = self._stats.num_messages_consumed // diff.total_seconds()

            logger.info(
                'consumer loop ending: total messages / sec = {}'.format(self._stats.total_throughput_per_second),
            )
            return self._stats


    def _consume_loop(self, max_msgs=None):
        num_batch_messages = 0
        self._stats.start_time = datetime.now(timezone.utc)
        self._stats.num_messages_consumed = 0

        self.handler.init()

        for msg in self.source.read():
            if msg is None:
                continue
            self._stats.num_messages_consumed += 1
            self.handler.write(msg.value().decode())
            num_batch_messages += 1

            if self._stats.num_messages_consumed % 10000 == 0:
                now = datetime.now(timezone.utc)
                diff = (now - self._stats.start_time)
                logger.info('{}: reqs / second'.format(
                    self._stats.num_messages_consumed // diff.total_seconds()),
                )

            if num_batch_messages == self._batch_size:
                # apply the pipeline
                batch = self.handler.invoke()
                for l in batch:
                    self.sink.write(l)

                # Only commit after all messages in batch are processed
                self.sink.flush()
                self.source.commit()

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


def build_managed_tables(conn, table_confs):
    managed_tables = []
    for table in table_confs:
        # windowed tables are the only supported tables currently
        if not table.manager:
            continue

        if not table.manager.tumbling_window:
            raise NotImplementedError('only tumbling_window manager currently supported')

        sink = new_sink_from_conf(table.manager.sink)

        h = window.Tumbling(
            conn=conn,
            table=window.Table(
                name=table.name,
                time_field=table.manager.tumbling_window.time_field,
            ),
            size_seconds=table.manager.tumbling_window.duration_seconds,
            sink=sink,
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


def new_sink_from_conf(sink_conf: config.Sink):
    if sink_conf.type == 'kafka':
        p = Producer({
            'bootstrap.servers': ','.join(sink_conf.kafka.brokers),
            'client.id': socket.gethostname(),
        })
        return KafkaSink(
            topic=sink_conf.kafka.topic,
            producer=p,
        )
    elif sink_conf.type == 'console':
        return ConsoleSink()

    raise NotImplementedError('unsupported sink type: {}'.format(sink_conf.type))


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

    raise NotImplementedError('unsupported source type: {}'.format(source_conf.type))


def new_sqlflow_from_conf(conf, conn, handler) -> SQLFlow:
    source = new_source_from_conf(conf.pipeline.source)
    sink = new_sink_from_conf(conf.pipeline.sink)
    sflow = SQLFlow(
        source=source,
        handler=handler,
        sink=sink,
        batch_size=conf.pipeline.batch_size,
    )

    return sflow

