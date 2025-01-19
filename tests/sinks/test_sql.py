import unittest
import tempfile
import os

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from sqlflow import config
from sqlflow.sinks import SQLCommandSink


class TestSQLCommandSink(unittest.TestCase):
    def test_local_parquet_multiple_values_single_file(self):
        conn = duckdb.connect()

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, 'test')
            os.mkdir(output_path)

            sink = SQLCommandSink(
                conn,
                sql=f"COPY sqlflow_sink_batch TO '{output_path}/$uuid.parquet' (FORMAT 'parquet')",
                substitutions=(
                    config.SQLCommandSubstitution(var='$uuid', type='uuid4'),
                )
            )

            table = pa.Table.from_pydict({
                "id": [1, 2],
                "name": ["Alice", "Bob"]
            })
            sink.write_table(table)

            # Flush the data to Parquet file
            sink.flush()

            # Check if the Parquet file is created
            files = os.listdir(output_path)
            parquet_files = [f for f in files if f.endswith('.parquet')]
            self.assertEqual(len(parquet_files), 1)

            # Read the Parquet file and verify the content
            parquet_file_path = os.path.join(output_path, parquet_files[0])
            table = pq.read_table(parquet_file_path)

            expected_data = [
                {'id': 1, 'name': 'Alice'},
                {'id': 2, 'name': 'Bob'}
            ]
            self.assertEqual(table.to_pylist(), expected_data)