import unittest
from flask import Flask
from sqlflow.http import DebugAPI
import duckdb
import threading

class DebugAPITestCase(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.conn = duckdb.connect(':memory:')
        self.lock = threading.Lock()
        self.app.add_url_rule(
            '/debug',
            view_func=DebugAPI.as_view(
                'sql_query',
                conn=self.conn,
                lock=self.lock,
            ),
        )
        self.client = self.app.test_client()

        # Create a sample table for testing
        with self.lock:
            self.conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
            self.conn.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")

    def test_no_sql_query_provided(self):
        response = self.client.get('/debug')
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json, {'error': 'No SQL query provided'})

    def test_valid_sql_query(self):
        response = self.client.get('/debug', query_string={'sql': 'SELECT * FROM test'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, [[1, 'Alice'], [2, 'Bob']])

    def test_invalid_sql_query(self):
        response = self.client.get('/debug', query_string={'sql': 'SELECT * FROM non_existing_table'})
        self.assertEqual(response.status_code, 500)
        self.assertIn('error', response.json)