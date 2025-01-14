import os
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType

from sqlflow.lifecycle import invoke
from sqlflow.config import new_from_dict, ConsoleSink, TumblingWindow, TableManager, Sink

dev_dir = os.path.join(
    os.path.dirname(__file__),
    '..',
    'dev',
)

conf_dir = os.path.join(dev_dir, 'config')
fixtures_dir = os.path.join(dev_dir, 'fixtures')


class InvokeExamplesTestCase(unittest.TestCase):
    def test_basic_agg_disk(self):
        conn = duckdb.connect()
        table = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'basic.agg.yml'),
            fixture=os.path.join(fixtures_dir, 'simple.json'),
        )
        self.assertEqual([
            {"city": "New York", "city_count": 28672},
            {"city": "Baltimore", "city_count": 28672},
        ], table.to_pylist())

    def test_basic_agg_mem(self):
        conn = duckdb.connect()
        table = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'basic.agg.mem.yml'),
            fixture=os.path.join(fixtures_dir, 'simple.json'),
        )
        self.assertEqual([
            {"city": "New York", "city_count": 28672},
            {"city": "Baltimore", "city_count": 28672},
        ], table.to_pylist())

    def test_csv_filesystem_join(self):
        conn = duckdb.connect()
        table = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'csv.filesystem.join.yml'),
            fixture=os.path.join(fixtures_dir, 'simple.json'),
            setting_overrides={
                'STATIC_ROOT': dev_dir,
            }
        )
        self.assertEqual([
            {"state_full": "Ohio", "city_count": 57344},
            {"state_full": "New York", "city_count": 1777664},
            {"state_full": "Maryland", "city_count": 1232896},
        ], table.to_pylist())

    def test_csv_mem_join(self):
        conn = duckdb.connect()
        table = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'csv.mem.join.yml'),
            fixture=os.path.join(fixtures_dir, 'window.jsonl'),
            setting_overrides={
                'STATIC_ROOT': dev_dir,
            }
        )
        self.assertEqual([
            {'city': 'New York', 'state_full': 'New York'},
            {'city': 'New York', 'state_full': 'New York'},
            {'city': 'Baltimore', 'state_full': 'Maryland'},
            {'city': 'Baltimore', 'state_full': 'Maryland'}
        ], table.to_pylist())

    def test_enrich(self):
        conn = duckdb.connect()
        table = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'enrich.yml'),
            fixture=os.path.join(fixtures_dir, 'enrich.jsonl'),
        )
        self.assertEqual([
           {"event": "search", "properties": {"city": "New York"}, "user": {"id": "123412ds"}, "nested_city": {"something": "New York"}, "extra": "extra"},
        ], table.to_pylist())

    def test_tumbling_window(self):
        conn = duckdb.connect()
        res = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'tumbling.window.yml'),
            fixture=os.path.join(fixtures_dir, 'window.jsonl'),
            flush_window=True,
        )

        self.assertEqual(
            [
                {'bucket': '2015-12-12T19:00:00', 'city': 'Baltimore', 'count': 2},
                {'bucket': '2015-12-12T19:00:00', 'city': 'New York', 'count': 2}
            ],
            res.to_pylist(),
        )

    def test_local_parquet_sink(self):
        conn = duckdb.connect()
        with tempfile.TemporaryDirectory() as temp_dir:
            table = invoke(
                conn=conn,
                config=os.path.join(conf_dir, 'examples', 'local.parquet.sink.yml'),
                fixture=os.path.join(fixtures_dir, 'window.jsonl'),
                invoke_sink=True,
                setting_overrides={
                    'sink_base_path': temp_dir,
                }
            )
            self.assertEqual([
                {"num_records": 4}
            ], table.to_pylist())

            files = os.listdir(temp_dir)
            parquet_files = [f for f in files if f.endswith('.parquet')]
            self.assertEqual(len(parquet_files), 1)

            # Read the Parquet file and verify the content
            parquet_file_path = os.path.join(temp_dir, parquet_files[0])
            table = pq.read_table(parquet_file_path)

            expected_data = [
                {'num_records': 4}
            ]
            self.assertEqual(table.to_pylist(), expected_data)

    def test_kafka_mem_iceberg(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            warehouse_path = temp_dir

            # Set up the catalog
            catalog = SqlCatalog(
                "test_catalog",
                **{
                    "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                    "warehouse": f"file://{warehouse_path}",
                },
            )

            catalog.create_namespace("default")

            schema = Schema(
                NestedField(field_id=1, name="city", field_type=StringType(), required=False),
                NestedField(field_id=2, name="count", field_type=IntegerType(), required=False),
            )

            iceberg_table = catalog.create_table(
                "default.city_events",
                schema=schema,
            )

            # Load the configuration from the YAML file
            config_path = os.path.join(conf_dir, 'examples', 'kafka.mem.iceberg.yml')

            with patch.dict(os.environ, {
                'PYICEBERG_CATALOG__TEST_CATALOG__URI': f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                'PYICEBERG_CATALOG__TEST_CATALOG__WAREHOUSE': f"file://{warehouse_path}",
            }):
                # Call invoke with the configuration
                conn = duckdb.connect()
                table = invoke(
                    conn=conn,
                    config=config_path,
                    fixture=os.path.join(fixtures_dir, 'window.jsonl'),
                    setting_overrides={
                        'catalog_name': 'test_catalog',
                        'table_name': 'city_events',
                    },
                    invoke_sink=True,
                )

            expected_data = [
                {'city': 'New York', 'city_count': 2},
                {'city': 'Baltimore', 'city_count': 2},
            ]
            self.assertEqual(table.to_pylist(), expected_data)

            import ipdb; ipdb.set_trace();
            pass


class TablesTestCase(unittest.TestCase):
    def test_init_window_success(self):
        conf = new_from_dict({
            'tables': {
                'sql': [
                    {
                        'name': 'test',
                        'sql': 'SELECT 1',
                        'manager': {
                            'tumbling_window': {
                                'collect_closed_windows_sql': 'SELECT 1',
                                'delete_closed_windows_sql': 'SELECT 1',
                            },
                            'sink': {
                                'type': 'console',
                            }
                        }

                    }
                ]
            },
            'pipeline': {
                'batch_size': 1000,
                'source': {
                    'type': 'kafka',
                    'kafka': {
                        'brokers': [],
                        'topics': [],
                        'group_id': 'test',
                        'auto_offset_reset': 'earliest',
                    }
                },
                'handler': {
                    'type': 'sql',
                    'sql': 'SELECT 1',
                },
                'sink': {
                    'type': 'console',
                }
            },
        })
        self.assertEqual(
            TableManager(
                tumbling_window=TumblingWindow(
                    collect_closed_windows_sql='SELECT 1',
                    delete_closed_windows_sql='SELECT 1',
                ),
                sink=Sink(
                    type='console',
                    console=ConsoleSink(),
                )
            ),
            conf.tables.sql[0].manager,
        )

