import unittest
import os

from sqlflow.config import Conf, Pipeline, Handler
from sqlflow.handlers import InferredMemBatch, InferredDiskBatch
from sqlflow.serde import JSON


class InferredMemBatchTestCase(unittest.TestCase):
    def test_flat(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredMemBatch(
            conf=Conf(
                pipeline=Pipeline(
                    batch_size=1000,
                    source=None,
                    handler=Handler(
                        type=None,
                        sql="SELECT COUNT(*) as num_rows FROM batch",
                    ),
                    sink=None,
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
                pipeline=Pipeline(
                    batch_size=1000,
                    source=None,
                    handler=Handler(
                        type=None,
                        sql="""
                        SELECT
                            {'something': city} as s1,
                            row(city, 1, 2) as nested_json
                        FROM batch 
                    """,
                    ),
                    sink=None,
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

        p = InferredDiskBatch(
            conf=Conf(
                pipeline=Pipeline(
                    batch_size=1000,
                    source=None,
                    handler=Handler(
                        type=None,
                        sql="SELECT COUNT(*) as num_rows FROM batch",
                    ),
                    sink=None,
                ),
            ),
            deserializer=JSON(),
        ).init()
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
        p = InferredDiskBatch(
            conf=Conf(
                pipeline=Pipeline(
                    batch_size=1000,
                    source=None,
                    handler=Handler(
                        type=None,
                        sql="""
                        SELECT
                            {'something': city} as s1,
                            row(city, 1, 2) as nested_json
                        FROM batch 
                    """,
                    ),
                    sink=None,
                ),
            ),
            deserializer=JSON(),
        ).init()

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
