import logging
import os
import sys
import uuid
import socket

import pyarrow as pa
from abc import abstractmethod, ABC

import duckdb
import pyiceberg.table
from confluent_kafka import Producer
from pyiceberg.catalog import load_catalog

from sqlflow import config
from sqlflow.serde import JSON

logger = logging.getLogger(__name__)


class Sink(ABC):
    @abstractmethod
    def write_table(self, table: pa.Table):
        """
        Writes a byte string to the underlying storage.

        :param val:
        :param key:
        :return:
        """
        raise NotImplemented()

    @abstractmethod
    def flush(self):
        """
        Flushes any buffered data to the underlying storage.

        :return:
        """
        pass


class ConsoleSink(Sink):
    def __init__(self, f=sys.stdout, serializer=JSON()):
        self.f = f
        self.serializer = serializer

    def write_table(self, table: pa.Table):
        for val in table.to_pylist():
            self.f.write(self.serializer.encode(val))
            self.f.write('\n')

    def flush(self):
        pass


class IcebergSink(Sink):
    def __init__(self, catalog, iceberg_table: pyiceberg.table.Table):
        self.catalog = catalog
        self.iceberg_table = iceberg_table
        self._tables = []

    def write_table(self, table):
        self._tables.append(table)

    def flush(self):
        if not self._tables:
            return

        table = pa.concat_tables(self._tables)
        self.iceberg_table.append(table)
        self._tables = []


# TODO(turbolytics): Make this generic once more sinks (such as s3/postgres/etc) are added
class LocalSink(Sink):
    def __init__(self, base_path, prefix, format='parquet', conn=None):
        self.base_path = base_path
        self.prefix = prefix
        self.tables = []
        self.conn = conn
        self.format = format
        if self.conn is None:
            self.conn = duckdb.connect()

    def write_table(self, table: pa.Table):
        self.tables.append(table)

    def flush(self):
        if not self.tables:
            return

        # Quick and dirty, will make this more generic as more formatters are added
        if self.format == 'parquet':
            # Convert buffer to a DuckDB table
            table = pa.concat_tables(self.tables)
            self.conn.register('buffer_table', table)

            # Generate a unique file name
            file_name = f"{self.prefix}-{uuid.uuid4()}.parquet"
            file_path = os.path.join(self.base_path, file_name)

            logger.debug(f"Writing {len(table)} records to Parquet file: {file_path}")
            # Write the table to a Parquet file
            self.conn.execute(f"COPY buffer_table TO '{file_path}' (FORMAT 'parquet')")
        else:
            raise NotImplementedError(f"Unsupported format: {self.format}")

        # Clear the buffer
        self.tables = []


class KafkaSink(Sink):
    def __init__(self, topic, producer, serializer=JSON()):
        self.topic = topic
        self.producer = producer
        self.serializer = serializer

    def write_table(self, table: pa.Table):
        for row in table.to_pylist():
            self.producer.produce(
                self.topic,
                value=self.serializer.encode(row),
            )

    def flush(self):
        self.producer.flush()


class NoopSink(Sink):
    def write_table(self, table: pa.Table):
        pass

    def flush(self):
        pass


class RecordingSink(Sink):
    def __init__(self):
        self.writes = []

    def write_table(self, table: pa.Table):
        self.writes.append(table)

    def flush(self):
        pass


def new_sink_from_conf(sink_conf: config.Sink) -> Sink:
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
    elif sink_conf.type == 'local':
        return LocalSink(
            base_path=sink_conf.local.base_path,
            prefix=sink_conf.local.prefix,
            format=sink_conf.format.type,
        )
    elif sink_conf.type == 'noop':
        return NoopSink()
    elif sink_conf.type == 'iceberg':
        catalog = load_catalog(sink_conf.iceberg.catalog_name)
        table = catalog.load_table(sink_conf.iceberg.table_name)

        return IcebergSink(
            catalog=sink_conf.iceberg.catalog_name,
            iceberg_table=table,
        )

    raise NotImplementedError('unsupported sink type: {}'.format(sink_conf.type))
