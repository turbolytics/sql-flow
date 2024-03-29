import json
import os

import duckdb
import pyarrow as pa


class InferredDiskBatch:
    """
    InferredDiskBatch buffers message batch to disk, and handles
    messages as implicit json.
    """

    def __init__(self, conf, *args, **kwargs):
        self.conf = conf
        self._f = None
        self._batch_file = os.path.join(
            self.conf.sql_results_cache_dir,
            'consumer_batch.json',
        )
        self._out_file = os.path.join(
            self.conf.sql_results_cache_dir,
            'out.json',
        )

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
            for l in self._invoke():
                yield l
        finally:
            duckdb.sql('DROP TABLE IF EXISTS batch')

    def _invoke(self):
        duckdb.sql(
            'CREATE TABLE batch AS SELECT * FROM read_json_auto(\'{}\')'.format(
                self._batch_file
            ),
        )

        duckdb.sql(
            "COPY ({}) TO '{}'".format(
                self.conf.pipeline.sql,
                self._out_file,
            )
        )

        with open(self._out_file, 'r') as f:
            for l in f:
                yield l.strip()


class InferredMemBatch:
    """
    InferredMemBatch buffers message batch to memory, and handles
    messages as implicit json.
    """

    def __init__(self, conf, deserializer=None):
        self.conf = conf
        self.rows = None
        self.deserializer = deserializer
        self.conn = duckdb.connect(":memory:")

    def init(self):
        self.rows = []
        return self

    def write(self, bs):
        o = self.deserializer.decode(bs)
        self.rows.append(o)

    def invoke(self):
        try:
            for l in self._invoke():
                yield l
        finally:
            duckdb.sql('DROP TABLE IF EXISTS batch')

    def _invoke(self):
        batch = pa.Table.from_pylist(self.rows)
        res = self.conn.sql(
            self.conf.pipeline.sql,
        )

        df = res.df()

        records = json.loads(
            df.to_json(
                orient='records',
                index=False,
            )
        )

        for rec in records:
            yield json.dumps(rec)


def get_class(s: str):
    if s == 'handlers.InferredDiskBatch':
        return InferredDiskBatch
    elif s == 'handlers.InferredMemBatch':
        return InferredMemBatch
    else:
        raise NotImplementedError()
