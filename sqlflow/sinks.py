import logging
import sys
import uuid
import socket
from typing import Optional

import pyarrow as pa
from abc import abstractmethod, ABC

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
    def batch(self) -> Optional[pa.Table]:
        raise NotImplemented()

    @abstractmethod
    def flush(self):
        """
        Flushes any buffered data to the underlying storage.

        :return:
        """
        raise NotImplemented()


class ConsoleSink(Sink):
    def __init__(self, f=sys.stdout, serializer=JSON()):
        self.f = f
        self.serializer = serializer
        self._tables = []

    def batch(self) -> Optional[pa.Table]:
        return pa.concat_tables(self._tables)

    def write_table(self, table):
        self._tables.append(table)

    def flush(self):
        if not self._tables:
            return

        table = pa.concat_tables(self._tables)
        for val in table.to_pylist():
            self.f.write(self.serializer.encode(val))
            self.f.write('\n')

        self._tables = []


class IcebergSink(Sink):
    def __init__(self, catalog, iceberg_table: pyiceberg.table.Table):
        self.catalog = catalog
        self.iceberg_table = iceberg_table
        self._tables = []

    def batch(self) -> Optional[pa.Table]:
        return pa.concat_tables(self._tables)

    def write_table(self, table):
        self._tables.append(table)

    def flush(self):
        if not self._tables:
            return

        table = pa.concat_tables(self._tables)
        self.iceberg_table.append(table)
        self._tables = []


class SQLCommandSink(Sink):
    def __init__(self, conn, sql, substitutions=()):
        self.conn = conn
        self.sql = sql
        self.tables = []
        self.substitutions = substitutions

    def batch(self) -> Optional[pa.Table]:
        return pa.concat_tables(self.tables)

    def write_table(self, table: pa.Table):
        self.tables.append(table)

    def flush(self):
        if not self.tables:
            return

        table = pa.concat_tables(self.tables)
        self.conn.register('sqlflow_sink_batch', table)
        sql = self._apply_substitutions()
        res = self.conn.execute(sql)
        self.tables = []

    def _apply_substitutions(self) -> str:
        sql = self.sql[:]
        for substitution in self.substitutions:
            if substitution.type == 'uuid4':
                sql = sql.replace(substitution.var, str(uuid.uuid4()))
            else:
                raise NotImplementedError(f"unsupported substitution type: {substitution.type}")
        return sql


class KafkaSink(Sink):
    def __init__(self, topic, producer, serializer=JSON()):
        self.topic = topic
        self.producer = producer
        self.serializer = serializer
        self._table = None

    def batch(self) -> Optional[pa.Table]:
        return self._table

    def write_table(self, table: pa.Table):
        self._table = table
        for row in self._table.to_pylist():
            self.producer.produce(
                self.topic,
                value=self.serializer.encode(row),
            )

    def flush(self):
        self.producer.flush()


class NoopSink(Sink):

    def batch(self) -> Optional[pa.Table]:
        return None

    def write_table(self, table: pa.Table):
        pass

    def flush(self):
        pass


class RecordingSink(Sink):
    def __init__(self):
        self.writes = []

    def batch(self) -> Optional[pa.Table]:
        return pa.concat_tables(self.writes)

    def write_table(self, table: pa.Table):
        self.writes.append(table)

    def flush(self):
        pass


def new_sink_from_conf(sink_conf: config.Sink, conn) -> Sink:
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
    elif sink_conf.type == 'sqlcommand':
        return SQLCommandSink(
            sql=sink_conf.sqlcommand.sql,
            conn=conn,
            substitutions=sink_conf.sqlcommand.substitutions,
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
