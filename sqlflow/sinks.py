import logging
import json
import os
import sys
import uuid
import pyarrow as pa
from abc import abstractmethod, ABC

import duckdb


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
    def write_table(self, table: pa.Table):
        for val in table:
            sys.stdout.write(val)
            sys.stdout.write('\n')

    def flush(self):
        pass


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
        self.table = pa.table({})


class KafkaSink(Sink):
    def __init__(self, topic, producer):
        self.topic = topic
        self.producer = producer

    def write_table(self, table: pa.Table):
        for row in table.to_pylist():
            self.producer.produce(self.topic, key=None, value=row)

    def flush(self):
        self.producer.flush()


class RecordingSink(Sink):
    def __init__(self):
        self.writes = []

    def write_table(self, table: pa.Table):
        self.writes.append(table)

    def flush(self):
        pass
