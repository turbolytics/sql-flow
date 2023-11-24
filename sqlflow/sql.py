import os

import duckdb


class InferredBatch:
    def __init__(self, conf):
        self.conf = conf

    def invoke(self, batch_file):
        duckdb.sql(
            'CREATE TABLE source AS SELECT * FROM read_json_auto(\'{}\')'.format(
                batch_file
            ),
        )

        out_file = os.path.join(
            self.conf.sql_results_cache_dir,
            'out.json',
        )

        duckdb.sql(
            "COPY ({}) TO '{}'".format(
                self.conf.pipeline.sql,
                out_file,
            )
        )

        with open(out_file, 'r') as f:
            for l in f:
                yield l.strip()