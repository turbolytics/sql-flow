import unittest
import tempfile
import os
import json
from unittest.mock import patch, MagicMock, call

import pyarrow as pa
import pyarrow.parquet as pq
from sqlflow.sinks import LocalSink, ConsoleSink, KafkaSink


class ConsoleWriterTestCase(unittest.TestCase):

    def test_write_single_val(self):
        table = pa.Table.from_pydict({"greeting": ["hello"]})
        mock_f = MagicMock()
        w = ConsoleSink(f=mock_f)

        w.write_table(table)

        mock_f.write.assert_has_calls([
            call('{"greeting": "hello"}'),
            call('\n'),
        ])

    def test_write_multiple_vals(self):
        table = pa.Table.from_pydict({
            "greeting": ["hello", "hi", "hey"],
            "name": ["Alice", "Bob", "Charlie"]
        })
        mock_f = MagicMock()
        w = ConsoleSink(f=mock_f)

        w.write_table(table)

        mock_f.write.assert_has_calls([
            call('{"greeting": "hello", "name": "Alice"}'),
            call('\n'),
            call('{"greeting": "hi", "name": "Bob"}'),
            call('\n'),
            call('{"greeting": "hey", "name": "Charlie"}'),
            call('\n'),
        ])


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


class TestLocalParquetSink(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_path = self.temp_dir.name
        self.prefix = 'test'
        self.sink = LocalSink(
            base_path=self.base_path,
            prefix=self.prefix,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_write_and_flush_multiple_values(self):
        table = pa.Table.from_pydict({
            "id": [1, 2],
            "name": ["Alice", "Bob"]
        })
        self.sink.write_table(table)

        # Flush the data to Parquet file
        self.sink.flush()

        # Check if the Parquet file is created
        files = os.listdir(self.base_path)
        parquet_files = [f for f in files if f.endswith('.parquet')]
        self.assertEqual(len(parquet_files), 1)

        # Read the Parquet file and verify the content
        parquet_file_path = os.path.join(self.base_path, parquet_files[0])
        table = pq.read_table(parquet_file_path)

        expected_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        self.assertEqual(table.to_pylist(), expected_data)