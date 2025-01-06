import json
import os
import sys
import uuid
import pyarrow as pa
from abc import abstractmethod, ABC

import duckdb


class Sink(ABC):
    @abstractmethod
    def write(self, val: bytes, key: bytes = None):
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
    def write(self, val: bytes, key: bytes = None):
        sys.stdout.write(val)
        sys.stdout.write('\n')

    def flush(self):
        pass



# TODO(turbolytics): Make this generic once more sinks (such as s3/postgres/etc) are added
class LocalSink(Sink):
    def __init__(self, base_path, prefix, format='parquet', conn=None):
        self.base_path = base_path
        self.prefix = prefix
        self.buffer = []
        self.conn = conn
        self.format = format
        if self.conn is None:
            self.conn = duckdb.connect()

    def write(self, val: bytes, key: bytes = None):
        self.buffer.append(val)

    def flush(self):
        if not self.buffer:
            return

        # Quick and dirty, will make this more generic as more formatters are added
        if self.format == 'parquet':
            # Convert buffer to a DuckDB table
            table = pa.Table.from_pylist([json.loads(b) for b in self.buffer])
            self.conn.register('buffer_table', table)

            # Generate a unique file name
            file_name = f"{self.prefix}-{uuid.uuid4()}.parquet"
            file_path = os.path.join(self.base_path, file_name)

            # Write the table to a Parquet file
            self.conn.execute(f"COPY buffer_table TO '{file_path}' (FORMAT 'parquet')")
        else:
            raise NotImplementedError(f"Unsupported format: {self.format}")

        # Clear the buffer
        self.buffer = []


class KafkaSink(Sink):
    def __init__(self, topic, producer):
        self.topic = topic
        self.producer = producer

    def write(self, val: bytes, key: bytes = None):
        self.producer.produce(self.topic, key=key, value=val.encode('utf-8'))

    def flush(self):
        self.producer.flush()


class RecordingSink(Sink):
    def __init__(self):
        self.writes = []

    def write(self, val: bytes, key: bytes = None):
        self.writes.append((key, val))

    def flush(self):
        pass
