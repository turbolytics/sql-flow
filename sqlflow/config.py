from yaml import safe_load
from dataclasses import dataclass


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
        pipeline=Pipeline(
            **conf['pipeline']
        )
    )