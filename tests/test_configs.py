import os
import unittest

import duckdb

from sqlflow import invoke

dev_dir = os.path.join(
    os.path.dirname(__file__),
    '..',
    'dev',
)

conf_dir = os.path.join(dev_dir, 'config')
fixtures_dir = os.path.join(dev_dir, 'fixtures')


class ExamplesTestCase(unittest.TestCase):
    def test_basic_agg(self):
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
            '{"state_full":"Ohio","city_count":57344}',
            '{"state_full":"New York","city_count":1777664}',
            '{"state_full":"Maryland","city_count":1232896}',
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
            '{"state_full":"Ohio","city_count":57344}',
            '{"state_full":"New York","city_count":1777664}',
            '{"state_full":"Maryland","city_count":1232896}',
        ], out)
