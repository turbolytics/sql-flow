import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.json as paj

logger = logging.getLogger(__name__)


class Handler(ABC):

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def write(self, bs):
        pass

    @abstractmethod
    def invoke(self) -> Optional[pa.Table]:
        pass



class InferredDiskBatch(Handler):
    """
    InferredDiskBatch buffers message batch to disk, and handles
    messages as  implicitjson.
    """

    def __init__(self, conf, *args, conn=None, **kwargs):
        self.conf = conf
        self._f = None
        self._batch_file = os.path.join(
            self.conf.pipeline.handler.sql_results_cache_dir,
            'consumer_batch.json',
        )
        self._out_file = os.path.join(
            self.conf.pipeline.handler.sql_results_cache_dir,
            'out.json',
        )
        self.conn = conn if conn else duckdb.connect(":memory:")

    def init(self):
        f = open(self._batch_file, 'w+')
        f.truncate()
        f.seek(0)
        self._f = f
        return self

    def write(self, bs):
        self._f.write(bs)
        self._f.write('\n')

    def invoke(self):
        self._f.flush()
        self._f.close()
        try:
            return self._invoke()
        finally:
            self.conn.execute('DROP TABLE IF EXISTS batch')

    def _invoke(self):
        self.conn.execute(
            'CREATE TABLE batch AS SELECT * FROM read_json_auto(\'{}\')'.format(
                self._batch_file
            ),
        )

        self.conn.execute(
            "COPY ({}) TO '{}'".format(
                self.conf.pipeline.handler.sql,
                self._out_file,
            )
        )

        table = paj.read_json(self._out_file)
        return table


class InferredMemBatch(Handler):
    """
    InferredMemBatch buffers message batch to memory, and handles
    messages as implicit json.
    """

    def __init__(self, conf, deserializer=None, conn=None):
        self.conf = conf
        self.rows = None
        self.deserializer = deserializer
        self.conn = conn if conn else duckdb.connect(":memory:")

    def init(self):
        self.rows = []
        return self

    def write(self, bs):
        o = self.deserializer.decode(bs)
        self.rows.append(o)

    def invoke(self) -> Optional[pa.Table]:
        return self._invoke()

    def _invoke(self):
        batch = pa.Table.from_pylist(self.rows)

        try:
            res = self.conn.execute(
                self.conf.pipeline.handler.sql,
            )
        except duckdb.duckdb.BinderException as e:
            logger.error(
                'could not execute sql: {}'.format(self.conf.pipeline.handler.sql),
            )
            raise e

        if not res:
            return

        return res.fetch_arrow_table()

def get_class(s: str):
    if s == 'handlers.InferredDiskBatch':
        return InferredDiskBatch
    elif s == 'handlers.InferredMemBatch':
        return InferredMemBatch
    else:
        raise NotImplementedError()
