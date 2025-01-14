import unittest
from unittest.mock import MagicMock, call

import pyarrow as pa

from sqlflow.sinks import KafkaSink


class KafkaWriterTestCase(unittest.TestCase):
    def test_write_table_single_row(self):
        producer = MagicMock()
        w = KafkaSink(
            topic='test',
            producer=producer,
        )

        table = pa.Table.from_pydict({"greeting": ["hello"]})
        w.write_table(table)
        producer.produce.assert_called_once_with(
            'test',
            value='{"greeting": "hello"}',
        )

    def test_write_table_multiple_rows(self):
        producer = MagicMock()
        w = KafkaSink(
            topic='test',
            producer=producer,
        )

        table = pa.Table.from_pydict({
            "greeting": ["hello", "hi", "hey"],
            "name": ["Alice", "Bob", "Charlie"]
        })
        w.write_table(table)

        producer.produce.assert_has_calls([
            call('test', value='{"greeting": "hello", "name": "Alice"}'),
            call('test', value='{"greeting": "hi", "name": "Bob"}'),
            call('test', value='{"greeting": "hey", "name": "Charlie"}'),
        ])
