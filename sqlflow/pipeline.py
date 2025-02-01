import importlib
import threading
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import logging
from typing import List

from opentelemetry import metrics

from sqlflow import config, sinks, sources
from sqlflow.managers import window
from sqlflow.sinks import Sink
from sqlflow.sources import Source

logger = logging.getLogger(__name__)
meter = metrics.get_meter('sqlflow.pipeline')

message_counter = meter.create_counter(
    name="message_count",
    description="Number of messages processed",
    unit="messages",
)

source_read_latency = meter.create_histogram(
    name="source_read_latency",
    description="Latency of reading a message from the source",
    unit="seconds",
)

sink_flush_latency = meter.create_histogram(
    name="sink_flush_latency",
    description="Latency of flushing data to the sink",
    unit="seconds",
)

sink_flush_num_rows = meter.create_gauge(
    name="sink_flush_num_rows",
    description="Number of rows flushed to the sink",
    unit="rows",
)

sink_flush_count = meter.create_counter(
    name="sink_flush_count",
    description="Number of times sink was flushed, corresponds to the # of batches processed",
    unit="flushes",
)

batch_processing_latency = meter.create_histogram(
    name="batch_processing_latency",
    description="Latency of processing a batch of data, from first message to flush",
    unit="seconds",
)


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
                 batch_size=1,
                 flush_interval_seconds=30,
                 lock=threading.Lock()):
        self.source = source
        self.sink = sink
        self.handler = handler
        self._batch_size = batch_size
        self._flush_interval_seconds = flush_interval_seconds
        self._stats = Stats(
            num_messages_consumed=0,
            num_errors=0,
            start_time=datetime.now(timezone.utc),
        )
        self._lock = lock
        self._running = True

    def consume_loop(self, max_msgs=None):
        logger.info('consumer loop starting')
        try:
            self.source.start()
            self._consume_loop(max_msgs)
        except StopIteration:
            logger.warning('loop stopping due to StopIteration')
        except Exception as e:
            logger.error('error in consumer loop: {}'.format(e))
            raise
        finally:
            self.source.close()

            now = datetime.now(timezone.utc)
            diff = (now - self._stats.start_time)
            try:
                self._stats.total_throughput_per_second = self._stats.num_messages_consumed // diff.total_seconds()
            except ZeroDivisionError:
                pass

            logger.info(
                'consumer loop ending: total messages / sec = {}'.format(self._stats.total_throughput_per_second),
            )
        return self._stats

    def _liveness_time(self):
        return datetime.now(timezone.utc)

    def _flush(self):
        sink_flush_start = datetime.now(timezone.utc)
        # Only commit after all messages in batch are processed
        try:
            self.sink.flush()
        except Exception as e:
            sink_batch = self.sink.batch()
            rows = sink_batch.to_pylist() if sink_batch is not None else []
            logger.error('{}: error flushing sink. With data: {}'.format(
                e,
                rows,
            ))
            self._stats.num_errors += 1
            raise e
        sink_flush_latency.record(
            (datetime.now(timezone.utc) - sink_flush_start).total_seconds(),
            attributes={
                'sink': self.sink.__class__.__name__,
            }
        )
        sink_flush_count.add(1, attributes={
            'sink': self.sink.__class__.__name__,
        })

    def _consume_loop(self, max_msgs=None):
        num_batch_messages = 0
        start_batch_time = None
        self._stats.start_time = datetime.now(timezone.utc)
        self._stats.num_messages_consumed = 0

        self.handler.init()

        # TODO: Flush interval seconds on background loop
        # Flush interval seconds only works right now if batch size is > 1.
        # Flush interval is implemented through the kafka poll timeout.
        # This means that the flush interval timeout is not supported in webhooks currently.
        # We'd like to minimize the use of threads. Currently kafka provides a workaround as part
        # of its implementation. If there's any adoption on the webhooks side, we can implement a
        # more sustainable and generic solution.
        liveness_timer_start = self._liveness_time()

        stream = self.source.stream()
        while self._running:
            source_read_start = datetime.now(timezone.utc)
            msg = next(stream)

            if msg is None:
                if datetime.now(timezone.utc) - liveness_timer_start > timedelta(seconds=self._flush_interval_seconds):
                    logger.debug('liveness check passed, issuing flush')
                    self._flush()
                    liveness_timer_start = self._liveness_time()
                continue

            source_read_latency.record(
                (datetime.now(timezone.utc) - source_read_start).total_seconds(),
                attributes={
                    'source': self.source.__class__.__name__,
                }
            )
            message_counter.add(1, attributes={
                'source': self.source.__class__.__name__,
            })
            # start the timer for to track the message batch latency
            if num_batch_messages == 0:
               start_batch_time = datetime.now(timezone.utc)

            self._stats.num_messages_consumed += 1
            try:
                self.handler.write(msg.value().decode())
            except Exception as e:
                logger.error('{}: error processing message: "{}"'.format(e, msg.value()))
                self._stats.num_errors += 1
                raise e
            num_batch_messages += 1

            if self._stats.num_messages_consumed % 10000 == 0:
                now = datetime.now(timezone.utc)
                diff = (now - self._stats.start_time)
                logger.info('{}: reqs / second'.format(
                    self._stats.num_messages_consumed // diff.total_seconds()),
                )

            if num_batch_messages == self._batch_size:
                # apply the pipeline
                with self._lock:
                    batch = self.handler.invoke()

                self.sink.write_table(batch)
                self._flush()
                self.source.commit()
                diff = (datetime.now(timezone.utc) - start_batch_time)
                batch_processing_latency.record(diff.total_seconds())

                # Send signal to handler indicating a new batch of data.
                self.handler.init()
                num_batch_messages = 0

            if max_msgs and max_msgs <= self._stats.num_messages_consumed:
                logger.info('max messages reached')
                return


def init_commands(conn, commands):
    for command in commands:
        logger.info('executing command {}'.format(command.name))
        conn.execute(command.sql)


def init_udfs(conn, udfs: List[config.UDF]):
    for udf in udfs:
        module_name, function_name = udf.import_path.rsplit('.', 1)
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)
        logger.info('creating udf: {}'.format(udf.function_name))
        conn.create_function(udf.function_name, function)


def init_tables(conn, tables):
    for sql_table in tables.sql:
        conn.execute(sql_table.sql)


def build_managed_tables(conn, table_confs, lock=threading.Lock()):
    managed_tables = []
    for table in table_confs:
        # windowed tables are the only supported tables currently
        if not table.manager:
            continue

        if not table.manager.tumbling_window:
            raise NotImplementedError('only tumbling_window manager currently supported')

        sink = sinks.new_sink_from_conf(table.manager.sink, conn)

        h = window.Tumbling(
            conn=conn,
            collect_closed_windows_sql=table.manager.tumbling_window.collect_closed_windows_sql,
            delete_closed_windows_sql=table.manager.tumbling_window.delete_closed_windows_sql,
            poll_interval_seconds=table.manager.tumbling_window.poll_interval_seconds,
            sink=sink,
            lock=lock,
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


def new_sqlflow_from_conf(conf, conn, handler, lock) -> SQLFlow:
    source = sources.new_source_from_conf(conf.pipeline.source)
    sink = sinks.new_sink_from_conf(conf.pipeline.sink, conn)
    sflow = SQLFlow(
        source=source,
        handler=handler,
        sink=sink,
        batch_size=conf.pipeline.batch_size,
        lock=lock,
    )

    return sflow

