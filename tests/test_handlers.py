import unittest
import os

from sqlflow.config import Conf, Pipeline, Handler
from sqlflow.handlers import InferredMemBatch, InferredDiskBatch
from sqlflow.serde import JSON


class InferredMemBatchTestCase(unittest.TestCase):
    def test_agg_batch_into_single_row(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredMemBatch(
            sql="SELECT COUNT(*) as num_rows FROM batch",
            deserializer=JSON(),
        ).init()
        with open(f_path) as f:
            for line in f:
                p.write(line)

        table = p.invoke()
        self.assertEqual(
            [{"num_rows": 3}],
            table.to_pylist(),
        )

    def test_inferred_batch_nested_return(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredMemBatch(
            sql="""
SELECT
    *,
    {'nested_city': city} AS enriched
FROM batch
                    """,
            deserializer=JSON(),
        ).init()

        with open(f_path) as f:
            for line in f:
                p.write(line)

        table = p.invoke()

        self.assertEqual([
            {"event": "search", "city": "New York", "user_id": "123412ds", "enriched": {"nested_city": "New York"}},
            {"event": "search", "city": "New York", "user_id": "123412ds", "enriched": {"nested_city": "New York"}},
            {"event": "search", "city": "Baltimore", "user_id": "123412ds", "enriched": {"nested_city": "Baltimore"}}
        ],
            table.to_pylist(),
        )


class InferredDiskBatchTestCase(unittest.TestCase):

    def test_inferred_batch_single_row_return(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')

        p = InferredDiskBatch(
            sql="SELECT COUNT(*) as num_rows FROM batch"
        ).init()
        with open(f_path) as f:
            for line in f:
                p.write(line)

        table = p.invoke()
        self.assertEqual(
            [{"num_rows": 3}],
            table.to_pylist(),
        )

    def test_inferred_batch_nested_return(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredDiskBatch(
            sql="""
SELECT
    {'city': city} as s1,
    {'event': event} as nested_event
FROM batch""",
        ).init()

        with open(f_path) as f:
            for line in f:
                p.write(line)

        table = p.invoke()

        self.assertEqual([
            {'s1': {'city': 'New York'}, 'nested_event': {'event': 'search'}},
            {'s1': {'city': 'New York'}, 'nested_event': {'event': 'search'}},
            {'s1': {'city': 'Baltimore'}, 'nested_event': {'event': 'search'}}
        ],
            table.to_pylist(),
        )
