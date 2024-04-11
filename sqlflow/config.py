import copy
from typing import Optional

from jinja2 import Template
from yaml import safe_load
from dataclasses import dataclass

from sqlflow import settings


@dataclass
class Window:
    type: str
    duration_seconds: int
    output: object


@dataclass
class CSVTable:
    name: str
    path: str
    header: bool
    auto_detect: bool


@dataclass
class SQLTable:
    name: str
    sql: str
    window: Optional[Window]


@dataclass
class Tables:
    csv: [CSVTable]
    sql: [SQLTable]


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
    return new_from_dict(conf)


def new_from_dict(conf):
    output_type = conf['pipeline'].get('output', {}).get('type')

    if output_type == 'kafka':
        output = KafkaOutput(
            type='kafka',
            topic=conf['pipeline']['output']['topic'],
        )
    else:
        output = ConsoleOutput(type='console')

    return Conf(
        kafka=Kafka(
            brokers=conf['kafka']['brokers'],
            group_id=conf['kafka']['group_id'],
            auto_offset_reset=conf['kafka']['auto_offset_reset'],
        ),
        tables=Tables(
            csv=[
                CSVTable(**t_conf) for t_conf in conf.get('tables', {}).get('csv', [])
            ],
            sql=[
                SQLTable(**sql_conf) for sql_conf in conf.get('tables', {}).get('sql', [])
            ]
        ),
        sql_results_cache_dir=conf.get('sql_results_cache_dir', settings.SQL_RESULTS_CACHE_DIR),
        pipeline=Pipeline(
            type=conf['pipeline'].get('type', 'handlers.InferredDiskBatch'),
            input=Input(
                batch_size=conf['pipeline']['input']['batch_size'],
                topics=conf['pipeline']['input']['topics'],
            ),
            sql=conf['pipeline']['sql'],
            output=output,
        )
    )

