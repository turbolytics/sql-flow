import unittest
from unittest.mock import MagicMock
import pyarrow as pa
from sqlflow.sinks import ClickhouseSink


class TestClickhouseSink(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.sink = ClickhouseSink(client=self.client, table_name='test_table')

    def test_write_table(self):
        table = pa.Table.from_pydict({"id": [1, 2], "name": ["Alice", "Bob"]})
        self.sink.write_table(table)
        self.assertEqual(self.sink._table, table)

    def test_flush(self):
        table = pa.Table.from_pydict({"id": [1, 2], "name": ["Alice", "Bob"]})
        self.sink.write_table(table)
        self.sink.flush()
        self.client.insert_arrow.assert_called_once_with('test_table', table)
        self.assertIsNone(self.sink._table)