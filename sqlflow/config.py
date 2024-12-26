import copy
from typing import Optional

from jinja2 import Template
from yaml import safe_load
from dataclasses import dataclass

from sqlflow import settings


@dataclass
class TumblingWindow:
    duration_seconds: int
    time_field: str


@dataclass
class TableManager:
    tumbling_window: Optional[TumblingWindow]
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
    manager: Optional[TableManager]


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


def build_output(c: object):
    output_type = c.get('type')
    if output_type == 'kafka':
        return KafkaOutput(
            type='kafka',
            topic=c['topic']
        )
    else:
        return ConsoleOutput(type='console')


def new_from_dict(conf):
    output = build_output(conf['pipeline'].get('output', {}))

    tables = Tables(
        csv=[],
        sql=[],
    )
    for csv_table in conf.get('tables', {}).get('csv', []):
        tables.csv.append(CSVTable(**csv_table))

    for sql_table_conf in conf.get('tables', {}).get('sql', []):
        manager_conf = sql_table_conf.pop('manager')
        if manager_conf:
            output = build_output(manager_conf.pop('output'))
            window_conf = manager_conf.pop('tumbling_window')
            s = SQLTable(
                manager=TableManager(
                    tumbling_window=TumblingWindow(
                        **window_conf,
                    ),
                    output=output,
                ),
                **sql_table_conf,
            )
            tables.sql.append(s)

    return Conf(
        kafka=Kafka(
            brokers=conf['kafka']['brokers'],
            group_id=conf['kafka']['group_id'],
            auto_offset_reset=conf['kafka']['auto_offset_reset'],
        ),
        tables=tables,
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

