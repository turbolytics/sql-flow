import os
import unittest

import duckdb

from sqlflow.lifecycle import invoke
from sqlflow.config import new_from_dict, ConsoleSink, TumblingWindow, TableManager

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
        out = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'basic.agg.yml'),
            fixture=os.path.join(fixtures_dir, 'simple.json'),
        )
        self.assertEqual([
            '{"city":"New York","city_count":28672}',
            '{"city":"Baltimore","city_count":28672}',
        ], out)

    def test_basic_agg_mem(self):
        conn = duckdb.connect()
        out = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'basic.agg.mem.yml'),
            fixture=os.path.join(fixtures_dir, 'simple.json'),
        )
        self.assertEqual([
            '{"city": "New York", "city_count": 28672}',
            '{"city": "Baltimore", "city_count": 28672}',
        ], out)

    def test_csv_filesystem_join(self):
        conn = duckdb.connect()
        out = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'csv.filesystem.join.yml'),
            fixture=os.path.join(fixtures_dir, 'simple.json'),
            setting_overrides={
                'STATIC_ROOT': dev_dir,
            }
        )
        self.assertEqual([
            '{"state_full": "Ohio", "city_count": 57344}',
            '{"state_full": "New York", "city_count": 1777664}',
            '{"state_full": "Maryland", "city_count": 1232896}',
        ], out)

    def test_csv_mem_join(self):
        conn = duckdb.connect()
        out = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'csv.mem.join.yml'),
            fixture=os.path.join(fixtures_dir, 'simple.json'),
            setting_overrides={
                'STATIC_ROOT': dev_dir,
            }
        )
        self.assertEqual([
            '{"state_full": "Ohio", "city_count": 57344}',
            '{"state_full": "New York", "city_count": 1777664}',
            '{"state_full": "Maryland", "city_count": 1232896}',
        ], out)

    def test_enrich(self):
        conn = duckdb.connect()
        out = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'enrich.yml'),
            fixture=os.path.join(fixtures_dir, 'enrich.jsonl'),
        )
        self.assertEqual([
           '{"event": "search", "properties": {"city": "New York"}, "user": {"id": "123412ds"}, "nested_city": {"something": "New York"}, "extra": "extra"}',
        ], out)

    def test_tumbling_window(self):
        conn = duckdb.connect()
        out = invoke(
            conn=conn,
            config=os.path.join(conf_dir, 'examples', 'tumbling.window.yml'),
            fixture=os.path.join(fixtures_dir, 'window.jsonl'),
            flush_window=True,
        )
        self.assertEqual(
            [
                {'timestamp': 1449878400000, 'city': 'New York', 'count': 2},
                {'timestamp': 1449878400000, 'city': 'Baltimore', 'count': 2},
            ],
            out,
        )


class TablesTestCase(unittest.TestCase):
    def test_init_window_success(self):
        conf = new_from_dict({
            'kafka': {
                'brokers': [],
                'group_id': 'test',
                'auto_offset_reset': 'earliest',
            },
            'tables': {
                'sql': [
                    {
                        'name': 'test',
                        'sql': 'SELECT 1',
                        'manager': {
                            'tumbling_window': {
                                'duration_seconds': 600,
                                'time_field': 'time',
                            },
                            'output': {
                                'type': 'console',
                            }
                        }

                    }
                ]
            },
            'pipeline': {
                'sql': 'SELECT 1',
                'input': {
                    'batch_size': 1000,
                    'topics': [],
                }

            },
        })
        self.assertEqual(
            TableManager(
                tumbling_window=TumblingWindow(
                    duration_seconds=600,
                    time_field='time',
                ),
                output=ConsoleSink(
                    type='console',
                ),
            ),
            conf.tables.sql[0].manager,
        )

