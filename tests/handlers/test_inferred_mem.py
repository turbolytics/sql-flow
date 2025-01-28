import os
import unittest

from sqlflow.handlers import InferredMemBatch
from sqlflow import serde


f_path = os.path.join(
    os.path.dirname(__file__),
    '..',
    'fixtures',
    'flat.json',
)


class InferredMemBatchTestCase(unittest.TestCase):
    def test_agg_batch_into_single_row(self):
        p = InferredMemBatch(
            sql="SELECT COUNT(*) as num_rows FROM batch",
            deserializer=serde.JSON(),
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
        p = InferredMemBatch(
            sql="""
SELECT
    *,
    {'nested_city': city} AS enriched
FROM batch
                    """,
            deserializer=serde.JSON(),
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
