import copy
from typing import Optional

from jinja2 import Template
from yaml import safe_load
from dataclasses import dataclass

from sqlflow import settings


@dataclass
class KafkaSink:
    brokers: [str]
    topic: str


@dataclass
class ConsoleSink:
    pass


@dataclass
class Sink:
    type: str
    kafka: Optional[KafkaSink] = None
    console: Optional[ConsoleSink] = None


@dataclass
class TumblingWindow:
    duration_seconds: int
    time_field: str


@dataclass
class TableCSV:
    name: str
    path: str
    header: bool
    auto_detect: bool


@dataclass
class TableManager:
    tumbling_window: Optional[TumblingWindow]
    sink: Sink


@dataclass
class TableSQL:
    name: str
    sql: str
    manager: Optional[TableManager]


@dataclass
class Tables:
    csv: [TableCSV]
    sql: [TableSQL]


@dataclass
class KafkaSource:
    brokers: [str]
    group_id: str
    auto_offset_reset: str
    topics: [str]



@dataclass
class Source:
    type: str
    kafka: Optional[KafkaSource] = None


@dataclass
class Handler:
    type: str
    sql: str
    sql_results_cache_dir: str = settings.SQL_RESULTS_CACHE_DIR


@dataclass
class Pipeline:
    batch_size: int
    source: Source
    handler: Handler
    sink: Sink


@dataclass
class Conf:
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


def build_source_config_from_dict(conf) -> Source:
    source = Source(
        type=conf['type'],
    )

    if source.type == 'kafka':
        source.kafka = KafkaSource(
            brokers=conf['kafka']['brokers'],
            group_id=conf['kafka']['group_id'],
            auto_offset_reset=conf['kafka']['auto_offset_reset'],
            topics=conf['kafka']['topics'],
        )
    else:
        source.type = 'console'

    return source


def build_sink_config_from_dict(conf) -> Sink:
    sink = Sink(
        type=conf['type'],
    )

    if sink.type == 'kafka':
        sink.kafka = KafkaSink(
            brokers=conf['kafka']['brokers'],
            topic=conf['kafka']['topic'],
        )
    else:
        sink.type = 'console'
        sink.console = ConsoleSink()

    return sink


def new_from_dict(conf):
    tables = Tables(
        csv=[],
        sql=[],
    )
    for csv_table in conf.get('tables', {}).get('csv', []):
        tables.csv.append(TableCSV(**csv_table))

    for sql_table_conf in conf.get('tables', {}).get('sql', []):
        manager_conf = sql_table_conf.pop('manager')
        if manager_conf:
            sink = build_sink_config_from_dict(manager_conf.pop('sink'))
            window_conf = manager_conf.pop('tumbling_window')
            s = TableSQL(
                manager=TableManager(
                    tumbling_window=TumblingWindow(
                        **window_conf,
                    ),
                    sink=sink,
                ),
                **sql_table_conf,
            )
            tables.sql.append(s)

    sink = build_sink_config_from_dict(conf['pipeline']['sink'])
    source = build_source_config_from_dict(conf['pipeline']['source'])

    return Conf(
        tables=tables,
        pipeline=Pipeline(
            batch_size=conf['pipeline']['batch_size'],
            source=source,
            handler=Handler(
                type=conf['pipeline']['handler']['type'],
                sql=conf['pipeline']['handler']['sql'],
            ),
            sink=sink,
        ),
    )

