import unittest
import os

from sqlflow.handlers import InferredDiskBatch


f_path = os.path.join(
    os.path.dirname(__file__),
    '..',
    'fixtures',
    'flat.json',
)


class InferredDiskBatchTestCase(unittest.TestCase):

    def test_inferred_batch_single_row_return(self):
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
