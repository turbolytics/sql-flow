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
            schema=pa.schema([('id', pa.string()), ('name', pa.string())])
        )
        h.init()

        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")
        h.write(b'{"id": "invalid_id", "name": "Alice"}')
        with self.assertRaises(duckdb.duckdb.ConversionException):
            h.invoke()

    def test_invalid_sql_on_invoke_parser_exception(self):
        conn = duckdb.connect(":memory:")
        h = handlers.StructuredBatch(
            sql="SELECT !* FROM test_table",
            table="test_table",
            deserializer=serde.JSON(),
            conn=conn,
            schema=pa.schema([('id', pa.int32()), ('name', pa.string())])
        )
        h.init()
        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        h.write(b'{"id": 1, "name": "Alice"}')
        with self.assertRaises(duckdb.duckdb.ParserException):
            h.invoke()

    def test_invalid_sql_on_invoke_unknown_table_catalog_exception(self):
        conn = duckdb.connect(":memory:")
        h = handlers.StructuredBatch(
            sql="SELECT * FROM unknown_table",
            table="test_table",
            deserializer=serde.JSON(),
            conn=conn,
            schema=pa.schema([('id', pa.int32()), ('name', pa.string())])
        )
        h.init()
        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        h.write(b'{"id": 1, "name": "Alice"}')
        with self.assertRaises(duckdb.duckdb.CatalogException):
            h.invoke()

    def test_no_results_returned_on_invoke(self):
        conn = duckdb.connect(":memory:")
        h = handlers.StructuredBatch(
            sql="SELECT * FROM test_table WHERE id > 10",
            table="test_table",
            deserializer=serde.JSON(),
            conn=conn,
            schema=pa.schema([('id', pa.int32()), ('name', pa.string())])
        )
        h.init()
        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        h.write(b'{"id": 1, "name": "Alice"}')
        h.write(b'{"id": 2, "name": "Bob"}')
        result = h.invoke()
        self.assertEqual([], result.to_pylist())

    def test_with_rows_returned_on_invoke(self):
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

        h.write(b'{"id": 1, "name": "Alice"}')
        h.write(b'{"id": 2, "name": "Bob"}')
        result = h.invoke()
        self.assertEqual([
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
        ], result.to_pylist())