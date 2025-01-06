import unittest
import tempfile
import os
import json
import pyarrow.parquet as pq
from sqlflow.sinks import LocalSink


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

    def test_write_and_flush(self):
        # Write some data
        data = [
            json.dumps({'id': 1, 'name': 'Alice'}).encode('utf-8'),
            json.dumps({'id': 2, 'name': 'Bob'}).encode('utf-8')
        ]
        for record in data:
            self.sink.write(record)

        # Flush the data to Parquet file
        self.sink.flush()

        # Check if the Parquet file is created
        files = os.listdir(self.base_path)
        parquet_files = [f for f in files if f.endswith('.parquet')]
        self.assertEqual(len(parquet_files), 1)

        # Read the Parquet file and verify the content
        parquet_file_path = os.path.join(self.base_path, parquet_files[0])
        table = pq.read_table(parquet_file_path)
        df = table.to_pandas()

        expected_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        self.assertEqual(df.to_dict(orient='records'), expected_data)