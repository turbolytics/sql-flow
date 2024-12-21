import unittest
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa

from sqlflow.outputs import ConsoleWriter, TestWriter
from sqlflow.window.handlers import Tumbling, Table


class TumblingTestCase(unittest.TestCase):
    def test_no_closed_results(self):
        conn = duckdb.connect()

        conn.sql('''
        CREATE TABLE test_table (
            timestamp TIMESTAMP,
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
            table=Table(
                name='test_table',
                time_field='timestamp',
            ),
            size_seconds=600,
            writer=ConsoleWriter(),
        )
        rows = tw.collect_closed()
        self.assertEqual([], rows)

    def test_closed_results_found(self):
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
            table=Table(
                name='test_table',
                time_field='timestamp',
            ),
            size_seconds=600,
            writer=ConsoleWriter(),
        )
        rows = tw.collect_closed()
        self.assertEqual(
            [
                {'timestamp': 1704049200, 'id': 'test_1'}
            ],
            rows,
        )

    def test_flush_results(self):
        writer = TestWriter()

        tw = Tumbling(
            conn=None,
            table=Table(
                name='test_table',
                time_field='timestamp',
            ),
            size_seconds=600,
            writer=writer,
        )
        records = [
            'first',
            'second',
        ]
        tw.flush(records)
        self.assertEqual(
            [
                (None, 'first'),
                (None, 'second'),
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
            table=Table(
                name='test_table',
                time_field='timestamp',
            ),
            size_seconds=600,
            writer=ConsoleWriter(),
        )

        tw.delete_closed(t=1704049200)
