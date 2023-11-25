from yaml import safe_load
from dataclasses import dataclass

from sqlflow import settings


@dataclass
class Kafka:
    brokers: [str]
    group_id: str
    auto_offset_reset: str


@dataclass
class KafkaOutput:
    type: str
    topic: str


@dataclass
class Input:
    batch_size: int
    topics: [str]


@dataclass
class ConsoleOutput:
    type: str


@dataclass
class Pipeline:
    type: str
    input: Input
    sql: str
    output: object


@dataclass
class Conf:
    kafka: Kafka
    sql_results_cache_dir: str
    pipeline: Pipeline


def new_from_path(path: str):
    with open(path, 'r') as f:
        conf = safe_load(f)

    output = ConsoleOutput(type='console')

    if conf['pipeline']['output']['type'] == 'kafka':
        output = KafkaOutput(
            type='kafka',
            topic=conf['pipeline']['output']['topic'],
        )

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
            output=output,
        )
    )
