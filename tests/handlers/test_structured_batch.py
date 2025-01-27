import unittest
import duckdb
import pyarrow as pa
from pyarrow.lib import ArrowInvalid
from sqlflow import serde, handlers


class TestStructuredBatch(unittest.TestCase):

    def test_invalid_json_decode_on_write(self):
        conn = duckdb.connect(":memory:")
        h = handlers.StructuredBatch(
            sql="SELECT * FROM test_table",
            table="test_table",
            deserializer=serde.JSON(),
            conn=conn,
            schema=pa.schema([('id', pa.int32()), ('name', pa.string())])
        )
        h.init()
        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        with self.assertRaises(ValueError):
            h.write(b'invalid json')

    def test_invalid_pyarrow_schema_on_invoke_single_row(self):
        conn = duckdb.connect(":memory:")
        h = handlers.StructuredBatch(
            sql="SELECT * FROM test_table",
            table="test_table",
            deserializer=serde.JSON(),
            conn=conn,
            schema=pa.schema([('id', pa.int32()), ('name', pa.string())])
        )
        h.init()

        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")
        h.write(b'{"id": "invalid_id", "name": "Alice"}')
        with self.assertRaises(ArrowInvalid):
            h.invoke()

    def test_invalid_insert_into_table_on_invoke_single_row(self):
        conn = duckdb.connect(":memory:")
        h = handlers.StructuredBatch(
            sql="SELECT * FROM test_table",
            table="test_table",
            deserializer=serde.JSON(),
            conn=conn,
            schema=pa.schema([('id', pa.int32()), ('name', pa.string())])
        )
        h.init()

        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        handler.rows = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        with self.assertRaises(duckdb.BinderException):
            conn.execute("INSERT INTO non_existing_table VALUES (1, 'Alice')")

    '''
    def test_invalid_sql_on_invoke(self):
        conn = duckdb.connect(":memory:")
        sql = "SELECT * FROM test_table"
        table = "test_table"
        deserializer = serde.JSON()
        schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        handler = StructuredBatch(sql, table, deserializer, conn, schema)
        handler.init()
        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        handler.rows = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        with self.assertRaises(duckdb.BinderException):
            conn.execute("SELECT * FROM non_existing_table")

    def test_no_results_returned_on_invoke(self):
        conn = duckdb.connect(":memory:")
        sql = "SELECT * FROM test_table"
        table = "test_table"
        deserializer = serde.JSON()
        schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        handler = StructuredBatch(sql, table, deserializer, conn, schema)
        handler.init()
        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        handler.rows = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        conn.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")
        result = handler.invoke()
        self.assertIsNone(result)

    def test_with_rows_returned_on_invoke(self):
        conn = duckdb.connect(":memory:")
        sql = "SELECT * FROM test_table"
        table = "test_table"
        deserializer = serde.JSON()
        schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        handler = StructuredBatch(sql, table, deserializer, conn, schema)
        handler.init()
        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        handler.rows = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        conn.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")
        result = handler.invoke()
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.num_rows, 2)
    '''