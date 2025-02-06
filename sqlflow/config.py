import copy
import os
from typing import Optional, List

from jinja2 import Template
from yaml import safe_load
from dataclasses import dataclass

from sqlflow import settings, errors


@dataclass
class Error:
    policy: errors.Policy = errors.Policy.RAISE


@dataclass
class SinkFormat:
    type: str


@dataclass
class IcebergSink:
    catalog_name: str
    table_name: str


@dataclass
class KafkaSink:
    brokers: [str]
    topic: str


@dataclass
class ConsoleSink:
    pass


@dataclass
class SQLCommandSubstitution:
    var: str
    type: str


@dataclass
class SQLCommandSink:
    sql: str
    substitutions: List[SQLCommandSubstitution] = ()


@dataclass
class Sink:
    type: str
    format: Optional[SinkFormat] = None
    kafka: Optional[KafkaSink] = None
    console: Optional[ConsoleSink] = None
    sqlcommand: Optional[SQLCommandSink] = None
    iceberg: Optional[IcebergSink] = None


@dataclass
class TumblingWindow:
    collect_closed_windows_sql: str
    delete_closed_windows_sql: str
    poll_interval_seconds: int = 10


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
    sql: [TableSQL]


@dataclass
class UDF:
    function_name: str
    import_path: str


@dataclass
class SQLCommand:
    name: str
    sql: str


@dataclass
class KafkaSource:
    brokers: [str]
    group_id: str
    auto_offset_reset: str
    topics: [str]


@dataclass
class WebsocketSource:
    uri: str


@dataclass
class Source:
    type: str
    kafka: Optional[KafkaSource] = None
    websocket: Optional[WebsocketSource] = None
    error: Optional[Error] = None


@dataclass
class Handler:
    type: str
    sql: str
    sql_results_cache_dir: str = settings.SQL_RESULTS_CACHE_DIR
    table: str = None


@dataclass
class Pipeline:
    source: Source
    handler: Handler
    sink: Sink
    batch_size: int | None = None
    flush_interval_seconds: int = 30


@dataclass
class Conf:
    pipeline: Pipeline
    tables: Optional[Tables] = ()
    udfs: Optional[List[UDF]] = ()
    commands: Optional[List[SQLCommand]] = ()

def render_config(path: str, setting_overrides={}) -> dict:
    with open(path) as f:
        template = Template(f.read())

    settings_vars = copy.deepcopy(settings.VARS)

    for key, value in os.environ.items():
        if key.startswith('SQLFLOW_'):
            settings_vars[key] = value

    for k, v in setting_overrides.items():
        settings_vars[k] = v

    rendered_template = template.render(
        **settings_vars
    )

    return safe_load(rendered_template)

def new_from_path(path: str, setting_overrides={}):
    """
    Initialize a new configuration instance
    directly from the filesystem.

    :param path:
    :return:

    """
    config_dict = render_config(path, setting_overrides)
    return new_from_dict(config_dict)


def build_source_config_from_dict(conf) -> Source:
    source = Source(
        type=conf['type'],
        error=Error(
            policy=conf.get('on_error', {}).get('policy', errors.Policy.RAISE),
        ),
    )

    if source.type == 'kafka':
        source.kafka = KafkaSource(
            brokers=conf['kafka']['brokers'],
            group_id=conf['kafka']['group_id'],
            auto_offset_reset=conf['kafka']['auto_offset_reset'],
            topics=conf['kafka']['topics'],
        )

    elif source.type == 'websocket':
        source.websocket = WebsocketSource(
            uri=conf['websocket']['uri'],
        )
    else:
       raise NotImplementedError('unsupported source type: {}'.format(source.type))

    return source


def build_sink_config_from_dict(conf) -> Sink:
    sink = Sink(
        type=conf['type'],
    )

    if 'format' in conf:
        sink.format = SinkFormat(
            type=conf['format']['type'],
        )
        assert sink.format.type in ('parquet',), "unsupported format: {}".format(sink.format.type)

    if sink.type == 'kafka':
        sink.kafka = KafkaSink(
            brokers=conf['kafka']['brokers'],
            topic=conf['kafka']['topic'],
        )
    elif sink.type == 'sqlcommand':
        sink.sqlcommand = SQLCommandSink(
            sql=conf['sqlcommand']['sql'],
            substitutions=[SQLCommandSubstitution(**s) for s in conf['sqlcommand'].get('substitutions', ())],
        )
        for substitution in sink.sqlcommand.substitutions:
            assert substitution.type in ('uuid4',), "unsupported substitution type: {}".format(substitution.type)
    elif sink.type == 'noop':
        pass
    elif sink.type == 'iceberg':
        sink.iceberg = IcebergSink(
            catalog_name=conf['iceberg']['catalog_name'],
            table_name=conf['iceberg']['table_name'],
        )
    else:
        sink.type = 'console'
        sink.console = ConsoleSink()

    return sink


def new_from_dict(conf):
    tables = Tables(
        sql=[],
    )

    udfs = []
    for udf in conf.get('udfs', []):
        udfs.append(UDF(**udf))

    commands = []
    for command_conf in conf.get('commands', []):
        commands.append(SQLCommand(**command_conf))

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
        commands=commands,
        tables=tables,
        udfs=udfs,
        pipeline=Pipeline(
            batch_size=conf['pipeline'].get('batch_size', 1),
            flush_interval_seconds=conf['pipeline'].get('flush_interval_seconds', 30),
            source=source,
            handler=Handler(
                type=conf['pipeline']['handler']['type'],
                sql=conf['pipeline']['handler']['sql'],
                sql_results_cache_dir=conf['pipeline']['handler'].get('sql_results_cache_dir', settings.SQL_RESULTS_CACHE_DIR),
                table=conf['pipeline']['handler'].get('table'),
            ),
            sink=sink,
        ),
    )

