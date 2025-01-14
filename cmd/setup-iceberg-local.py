import os
import shutil

import click
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    StringType,
    NestedField,
)

from sqlflow import logging


@click.group()
def cli():
    pass


@click.command(name='setup')
@click.option(
    '--warehouse-path',
    type=str,
    default='/tmp/sqlflow/warehouse',
    help='Path to the Iceberg warehouse.',
)
@click.option(
    '--reinit',
    type=bool,
    default=True,
    help='Reinitialize the catalog by deleting any existing one.',
)
def setup(warehouse_path, reinit):
    # Create a temporary location for Iceberg
    if reinit:
        try:
            shutil.rmtree(warehouse_path)
        except FileNotFoundError:
            pass

    os.makedirs(warehouse_path, exist_ok=True)

    # Set up the catalog
    catalog = SqlCatalog(
        "sqlflow_test",
        **{
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass


    # create the city events schema
    schema = Schema(
        NestedField(field_id=1, name="timestamp", field_type=TimestampType(), required=False),
        NestedField(field_id=2, name="city", field_type=StringType(), required=False),
    )

    try:
        table = catalog.create_table(
            "default.city_events",
            schema=schema,
        )
    except TableAlreadyExistsError:
        pass

    print('created default.city_events')

    # create the blueksy schema
    schema = Schema(
        NestedField(field_id=1, name="timestamp", field_type=TimestampType(), required=False),
        NestedField(field_id=2, name="kind", field_type=StringType(), required=False),
    )

    try:
        table = catalog.create_table(
            "default.bluesky_post_events",
            schema=schema,
        )
    except TableAlreadyExistsError:
        pass
    print('created default.bluesky_post_events')

    print("Catalog setup complete.")


cli.add_command(setup)

if __name__ == '__main__':
    logging.init()
    cli()
