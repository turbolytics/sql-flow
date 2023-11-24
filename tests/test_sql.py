import unittest
import os

from sqlflow import InferredBatch, settings
from sqlflow.config import Conf, Pipeline


class InferredBatchTestCase(unittest.TestCase):

    def test_inferred_batch_flat(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredBatch(conf=Conf(
            sql_results_cache_dir=settings.SQL_RESULTS_CACHE_DIR,
            pipeline=Pipeline(
                sql="SELECT COUNT(*) as num_rows FROM batch"
            ),
        ))
        res = list(p.invoke(f_path))
        self.assertEqual(
            ['{"num_rows":3}'],
            res,
        )

    def test_inferred_batch_nested_return(self):
        f_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'flat.json')
        p = InferredBatch(conf=Conf(
            sql_results_cache_dir=settings.SQL_RESULTS_CACHE_DIR,
            pipeline=Pipeline(
                sql="""
                    SELECT
                        {'something': city} as s1,
                        row(city, 1, 2) as nested_json
                    FROM batch 
                """
            ),
        ))
        res = list(p.invoke(f_path))
        self.assertEqual([
            '{"s1":{"something":"New York"},"nested_json":{"":"New York","":1,"":2}}',
            '{"s1":{"something":"New York"},"nested_json":{"":"New York","":1,"":2}}',
            '{"s1":{"something":"Baltimore"},"nested_json":{"":"Baltimore","":1,"":2}}',
        ],
            res,
        )
