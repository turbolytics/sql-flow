import os
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)

# Create a temporary location for Iceberg
warehouse_path = "/tmp/warehouse"
os.makedirs(warehouse_path, exist_ok=True)

# Set up the catalog
catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

try:
    catalog.create_namespace("default")
except NamespaceAlreadyExistsError:
    pass


schema = Schema(
    NestedField(field_id=1, name="time", field_type=TimestampType(), required=True),
    NestedField(field_id=2, name="city", field_type=StringType(), required=True),
    NestedField(field_id=3, name="event", field_type=StringType(), required=True),
)

try:
    table = catalog.create_table(
        "default.city_events",
        schema=schema,
    )
except TableAlreadyExistsError:
    pass


print("Catalog setup complete.")

