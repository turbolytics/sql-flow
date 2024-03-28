import os

import duckdb


class InferredDiskBatch:
    """
    InferredDiskBatch buffers message batch to disk, and handles
    messages as implicit json.
    """

    def __init__(self, conf):
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


def get_class(s: str):
    if s == 'handlers.InferredDiskBatch':
        return InferredDiskBatch
    else:
        raise NotImplementedError()
