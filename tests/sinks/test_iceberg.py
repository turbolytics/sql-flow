import unittest
import tempfile
from datetime import datetime

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, StringType, NestedField
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from sqlflow.sinks import IcebergSink


class TestIcebergSink(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.warehouse_path = self.temp_dir.name

        # Set up the catalog
        self.catalog = SqlCatalog(
            "default",
            **{
                "uri": f"sqlite:///{self.warehouse_path}/pyiceberg_catalog.db",
                "warehouse": f"file://{self.warehouse_path}",
            },
        )

        try:
            self.catalog.create_namespace("default")
        except NamespaceAlreadyExistsError:
            pass

        self.schema = Schema(
            NestedField(field_id=1, name="time", field_type=TimestampType(), required=False),
            NestedField(field_id=2, name="city", field_type=StringType(), required=False),
            NestedField(field_id=3, name="event", field_type=StringType(), required=False),
        )

        try:
            self.table = self.catalog.create_table(
                "default.city_events",
                schema=self.schema,
            )
        except TableAlreadyExistsError:
            self.table = self.catalog.load_table("default.city_events")

        self.sink = IcebergSink(
            catalog=self.catalog,
            iceberg_table=self.table,)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_write_and_read(self):
        # Create a PyArrow table
        table = pa.Table.from_pydict({
            "time": [datetime(2023, 1, 1, 12, 0, 0)],
            "city": ["New York"],
            "event": ["search"]
        })

        # Write the table to the Iceberg sink
        self.sink.write_table(table)
        self.sink.flush()

        # Read the data back from the Iceberg table
        read_table = self.table.scan().to_arrow()

        # Verify the content
        expected_data = [
            {"time": datetime(2023, 1, 1, 12, 0, 0), "city": "New York", "event": "search"}
        ]
        self.assertEqual(read_table.to_pylist(), expected_data)

