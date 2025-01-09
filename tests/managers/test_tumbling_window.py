import unittest
from datetime import UTC, datetime, timedelta, timezone

import duckdb
import pyarrow as pa

from sqlflow.sinks import ConsoleSink, RecordingSink
from sqlflow.managers.window import Tumbling, Table


class TumblingWindowTestCase(unittest.TestCase):
    def test_no_closed_results(self):
        conn = duckdb.connect()

        conn.sql('''
        CREATE TABLE test_table (
            timestamp TIMESTAMPTZ,
            id VARCHAR
        )
        ''')

        # create df with time right now
        test_batch = pa.Table.from_pylist([
            {
                'timestamp': datetime.now(tz=UTC),
                'id': 'test_1',
            },
        ])
        conn.sql('''
        INSERT INTO test_table
        SELECT * FROM test_batch
        ''')

        tw = Tumbling(
            conn=conn,
            collect_closed_windows_sql='SELECT * FROM test_table WHERE timestamp < NOW() - INTERVAL 10 MINUTE',
            delete_closed_windows_sql=None,
            poll_interval_seconds=10,
            sink=ConsoleSink(),
        )
        rows = tw.collect_closed()
        self.assertEqual([], rows)

    def test_closed_results_found(self):
        conn = duckdb.connect()
        conn.execute("SET timezone = 'UTC'")

        conn.sql('''
        CREATE TABLE test_table (
            timestamp TIMESTAMPTZ,
            id VARCHAR
        )
        ''')

        d = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # create df with time right now
        test_batch = pa.Table.from_pylist([
            {
                'timestamp': d,
                'id': 'test_1',
            },
        ])
        conn.sql('''
        INSERT INTO test_table
        SELECT * FROM test_batch
        ''')

        tw = Tumbling(
            conn=conn,
            collect_closed_windows_sql='''
SELECT 
    strftime(timestamp, '%Y-%m-%dT%H:%M:%S') as timestamp, 
    id 
FROM test_table
            ''',
            delete_closed_windows_sql=None,
            poll_interval_seconds=10,
            sink=ConsoleSink(),
        )

        rows = tw.collect_closed()
        self.assertEqual(
            [
                {'timestamp': '2024-01-01T00:00:00', 'id': 'test_1'}
            ],
            rows,
        )

    def test_flush_results(self):
        writer = RecordingSink()

        tw = Tumbling(
            conn=None,
            collect_closed_windows_sql=None,
            delete_closed_windows_sql=None,
            poll_interval_seconds=10,
            sink=writer,
        )
        records = [
            'first',
            'second',
        ]
        tw.flush(records)
        self.assertEqual(
            [
                (None, '"first"'),
                (None, '"second"'),
            ],
            writer.writes,
        )

    def test_delete_results(self):
        conn = duckdb.connect()

        conn.sql('''
        CREATE TABLE test_table (
            timestamp TIMESTAMP,
            id VARCHAR
        )
        ''')

        d = datetime(2024, 1, 1, 0, 0, 0, 0, UTC)

        # create df with time right now
        test_batch = pa.Table.from_pylist([
            {
                'timestamp': d,
                'id': 'test_1',
            },
        ])
        conn.sql('''
        INSERT INTO test_table
        SELECT * FROM test_batch
        ''')

        tw = Tumbling(
            conn=conn,
            collect_closed_windows_sql=None,
            delete_closed_windows_sql='DELETE FROM test_table WHERE timestamp <= NOW()',
            poll_interval_seconds=10,
            sink=ConsoleSink(),
        )

        num_deleted = tw.delete_closed()
        self.assertEqual(1, num_deleted)
