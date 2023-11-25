from yaml import safe_load
from dataclasses import dataclass

from sqlflow import settings


@dataclass
class Kafka:
    brokers: [str]
    group_id: str
    auto_offset_reset: str


@dataclass
class Input:
    batch_size: int
    topics: [str]


@dataclass
class Output:
    type: str


@dataclass
class Pipeline:
    type: str
    input: Input
    sql: str
    output: Output


@dataclass
class Conf:
    kafka: Kafka
    sql_results_cache_dir: str
    pipeline: Pipeline


def new_from_path(path: str):
    with open(path, 'r') as f:
        conf = safe_load(f)

    return Conf(
        kafka=Kafka(
            brokers=conf['kafka']['brokers'],
            group_id=conf['kafka']['group_id'],
            auto_offset_reset=conf['kafka']['auto_offset_reset'],
        ),
        sql_results_cache_dir=conf.get('sql_results_cache_dir', settings.SQL_RESULTS_CACHE_DIR),
        pipeline=Pipeline(
            type=conf['pipeline'].get('type', 'inferred'),
            input=Input(
                batch_size=conf['pipeline']['input']['batch_size'],
                topics=conf['pipeline']['input']['topics'],
            ),
            sql=conf['pipeline']['sql'],
            output=conf['pipeline']['output'],
        )
    )
