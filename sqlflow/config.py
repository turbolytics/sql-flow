from yaml import safe_load
from dataclasses import dataclass

from sqlflow import settings


@dataclass
class Pipeline:
    sql: str


@dataclass
class Conf:
    sql_results_cache_dir: str
    pipeline: Pipeline


def new_from_path(path: str):
    with open(path, 'r') as f:
        conf = safe_load(f)

    return Conf(
        sql_results_cache_dir=conf['sql_results_cache_dir'] or settings.SQL_RESULTS_CACHE_DIR,
        pipeline=Pipeline(
            **conf['pipeline']
        )
    )
