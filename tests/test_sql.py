import unittest
import os

from sqlflow import settings
from sqlflow.config import Conf, Pipeline
from sqlflow.handlers import InferredMemBatch, InferredDiskBatch
from sqlflow.serde import JSON


class InferredMemBatchTestCase(unittest.TestCase):
    def test_flat(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredMemBatch(
            conf=Conf(
                kafka=None,
                sql_results_cache_dir=settings.SQL_RESULTS_CACHE_DIR,
                pipeline=Pipeline(
                    type=None,
                    input=None,
                    sql="SELECT COUNT(*) as num_rows FROM batch",
                    output=None,
                ),
            ),
            deserializer=JSON(),
        ).init()
        with open(f_path) as f:
            for line in f:
                p.write(line)

        res = list(p.invoke())
        self.assertEqual(
            ['{"num_rows": 3}'],
            res,
        )

    def test_inferred_batch_nested_return(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredMemBatch(
            conf=Conf(
                kafka=None,
                sql_results_cache_dir=settings.SQL_RESULTS_CACHE_DIR,
                pipeline=Pipeline(
                    type=None,
                    input=None,
                    sql="""
                        SELECT
                            {'something': city} as s1,
                            row(city, 1, 2) as nested_json
                        FROM batch 
                    """,
                    output=None,
                ),
            ),
            deserializer=JSON(),
        ).init()

        with open(f_path) as f:
            for line in f:
                p.write(line)

        res = list(p.invoke())

        self.assertEqual([
            '{"s1": {"something": "New York"}, "nested_json": {"": 2}}',
            '{"s1": {"something": "New York"}, "nested_json": {"": 2}}',
            '{"s1": {"something": "Baltimore"}, "nested_json": {"": 2}}',
        ],
            res,
        )


class InferredDiskBatchTestCase(unittest.TestCase):

    def test_inferred_batch_flat(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredDiskBatch(conf=Conf(
            kafka=None,
            sql_results_cache_dir=settings.SQL_RESULTS_CACHE_DIR,
            pipeline=Pipeline(
                type=None,
                input=None,
                sql="SELECT COUNT(*) as num_rows FROM batch",
                output=None,
            ),
        )).init()
        with open(f_path) as f:
            for line in f:
                p.write(line)

        res = list(p.invoke())
        self.assertEqual(
            ['{"num_rows":3}'],
            res,
        )

    def test_inferred_batch_nested_return(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredDiskBatch(conf=Conf(
            kafka=None,
            sql_results_cache_dir=settings.SQL_RESULTS_CACHE_DIR,
            pipeline=Pipeline(
                type=None,
                input=None,
                sql="""
                    SELECT
                        {'something': city} as s1,
                        row(city, 1, 2) as nested_json
                    FROM batch 
                """,
                output=None,
            ),
        )).init()

        with open(f_path) as f:
            for line in f:
                p.write(line)

        res = list(p.invoke())

        # TODO! Figure out this json with multiple "" keys
        self.assertEqual([
            '{"s1":{"something":"New York"},"nested_json":{"":"New York","":1,"":2}}',
            '{"s1":{"something":"New York"},"nested_json":{"":"New York","":1,"":2}}',
            '{"s1":{"something":"Baltimore"},"nested_json":{"":"Baltimore","":1,"":2}}',
        ],
            res,
        )
