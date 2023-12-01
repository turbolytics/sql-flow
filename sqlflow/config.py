import copy
from typing import Optional

from jinja2 import Template
from yaml import safe_load
from dataclasses import dataclass

from sqlflow import settings


@dataclass
class CSVTable:
    name: str
    path: str
    header: bool
    auto_detect: bool


@dataclass
class Tables:
    csv: [CSVTable]


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
    tables: Optional[Tables] = ()


def new_from_path(path: str, setting_overrides={}):
    """
    Initialize a new configuration instance
    directly from the filesystem.

    :param path:
    :return:
    """
    with open(path) as f:
        template = Template(f.read())

    settings_vars = copy.deepcopy(settings.VARS)
    for k, v in setting_overrides.items():
        settings_vars[k] = v

    rendered_template = template.render(
        **settings_vars
    )

    conf = safe_load(rendered_template)

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
        tables=Tables(
            csv=[
                CSVTable(**t_conf) for t_conf in conf.get('tables', {}).get('csv', [])
            ]
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
