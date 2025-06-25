import copy
import json
import os
from typing import Optional, List

from jinja2 import Template
from yaml import safe_load
from dataclasses import dataclass

from sqlflow import settings, errors


@dataclass
class KafkaSSLConfig:
    ca_location: str  # Path to the CA certificate
    certificate_location: str  # Path to the client certificate
    key_location: str  # Path to the client private key
    key_password: Optional[str] = None
    endpoint_identification_algorithm: Optional[str] = None  # e.g., https, none (default is https)


@dataclass
class KafkaSASLConfig:
    mechanism: str  # SASL mechanism (e.g., PLAIN, SCRAM-SHA-256)
    username: str  # SASL username
    password: str  # SASL password


@dataclass
class HMACConfig:
    header: str  # Header name for the HMAC signature
    sig_key: str  # Key used for HMAC signature validation
    secret: str # Shared Secret


@dataclass
class WebhookSource:
    signature_type: Optional[str] = None  # Type of signature validation (e.g., 'hmac')
    hmac: Optional[HMACConfig] = None


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
    security_protocol: Optional[str] = None  # Security protocol (e.g., PLAINTEXT, SSL, SASL_SSL)
    ssl: Optional[KafkaSSLConfig] = None  # SSL configuration
    sasl: Optional[KafkaSASLConfig] = None  # SASL configuration


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
class ClikhouseSink:
    dsn: str
    table: str


@dataclass
class Sink:
    type: str
    format: Optional[SinkFormat] = None
    kafka: Optional[KafkaSink] = None
    console: Optional[ConsoleSink] = None
    sqlcommand: Optional[SQLCommandSink] = None
    iceberg: Optional[IcebergSink] = None
    clickhouse: Optional[ClikhouseSink] = None


@dataclass
class DLQErrorPolicy:
    sink: Sink


@dataclass
class ErrorPolicy:
    dlq: Optional[DLQErrorPolicy] = None
    policy: errors.Policy = errors.Policy.RAISE


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
    security_protocol: Optional[str] = None  # Security protocol (e.g., PLAINTEXT, SSL, SASL_SSL)
    ssl: Optional[KafkaSSLConfig] = None  # SSL configuration
    sasl: Optional[KafkaSASLConfig] = None  # SASL configuration


@dataclass
class WebsocketSource:
    uri: str


@dataclass
class Source:
    type: str
    kafka: Optional[KafkaSource] = None
    websocket: Optional[WebsocketSource] = None
    webhook: Optional[WebhookSource] = None


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
    on_error: Optional[ErrorPolicy] = None


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
    )

    if source.type == 'kafka':
        ssl_config = None
        if 'ssl' in conf['kafka']:
            ssl_config = KafkaSSLConfig(
                ca_location=conf['kafka']['ssl'].get('ca_location'),
                certificate_location=conf['kafka']['ssl'].get('certificate_location'),
                key_location=conf['kafka']['ssl'].get('key_location'),
                key_password=conf['kafka']['ssl'].get('key_password'),
                endpoint_identification_algorithm=conf['kafka']['ssl'].get('endpoint_identification_algorithm'),
            )

        sasl_config = None
        if 'sasl' in conf['kafka']:
            sasl_config = KafkaSASLConfig(
                mechanism=conf['kafka']['sasl']['mechanism'],
                username=conf['kafka']['sasl']['username'],
                password=conf['kafka']['sasl']['password'],
            )

        source.kafka = KafkaSource(
            brokers=conf['kafka']['brokers'],
            group_id=conf['kafka']['group_id'],
            auto_offset_reset=conf['kafka']['auto_offset_reset'],
            topics=conf['kafka']['topics'],
            security_protocol=conf['kafka'].get('security_protocol'),
            ssl=ssl_config,
            sasl=sasl_config,
        )

    elif source.type == 'websocket':
        source.websocket = WebsocketSource(
            uri=conf['websocket']['uri'],
        )
    elif source.type == 'webhook':
        source.webhook = WebhookSource(
            signature_type=conf['webhook'].get('signature_type'),
            hmac=HMACConfig(
                header=conf['webhook']['hmac']['header'],
                sig_key=conf['webhook']['hmac']['sig_key'],
                secret=conf['webhook']['hmac']['secret'],
            ) if 'hmac' in conf['webhook'] else None,
        )
    else:
       raise NotImplementedError('unsupported source type: {}'.format(source.type))

    return source


def build_sink_config_from_dict(raw_conf) -> Sink:
    sink = Sink(
        type=raw_conf['type'],
    )

    if 'format' in raw_conf:
        sink.format = SinkFormat(
            type=raw_conf['format']['type'],
        )
        assert sink.format.type in ('parquet',), "unsupported format: {}".format(sink.format.type)

    if sink.type == 'kafka':
        ssl_config = None
        if 'ssl' in raw_conf['kafka']:
            ssl_config = KafkaSSLConfig(
                ca_location=raw_conf['kafka']['ssl'].get('ca_location'),
                certificate_location=raw_conf['kafka']['ssl'].get('certificate_location'),
                key_location=raw_conf['kafka']['ssl'].get('key_location'),
                key_password=raw_conf['kafka']['ssl'].get('key_password'),
                endpoint_identification_algorithm=raw_conf['kafka']['ssl'].get('endpoint_identification_algorithm'),
            )

        sasl_config = None
        if 'sasl' in raw_conf['kafka']:
            sasl_config = KafkaSASLConfig(
                mechanism=raw_conf['kafka']['sasl']['mechanism'],
                username=raw_conf['kafka']['sasl']['username'],
                password=raw_conf['kafka']['sasl']['password'],
            )

        sink.kafka = KafkaSink(
            brokers=raw_conf['kafka']['brokers'],
            topic=raw_conf['kafka']['topic'],
            security_protocol=raw_conf['kafka'].get('security_protocol'),
            ssl=ssl_config,
            sasl=sasl_config,
        )

    elif sink.type == 'sqlcommand':
        sink.sqlcommand = SQLCommandSink(
            sql=raw_conf['sqlcommand']['sql'],
            substitutions=[SQLCommandSubstitution(**s) for s in raw_conf['sqlcommand'].get('substitutions', ())],
        )
        for substitution in sink.sqlcommand.substitutions:
            assert substitution.type in ('uuid4',), "unsupported substitution type: {}".format(substitution.type)
    elif sink.type == 'noop':
        pass
    elif sink.type == 'iceberg':
        sink.iceberg = IcebergSink(
            catalog_name=raw_conf['iceberg']['catalog_name'],
            table_name=raw_conf['iceberg']['table_name'],
        )
    elif sink.type == 'clickhouse':
        sink.clickhouse = ClikhouseSink(
            dsn=raw_conf['clickhouse']['dsn'],
            table=raw_conf['clickhouse']['table'],
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

    error_policy_conf = conf['pipeline'].get('on_error', {})
    dlq_sink = None
    if 'dlq' in error_policy_conf:
        dlq_sink = build_sink_config_from_dict(error_policy_conf['dlq'])

    error_policy = ErrorPolicy(
        dlq=DLQErrorPolicy(sink=dlq_sink) if dlq_sink else None,
        policy=errors.Policy[error_policy_conf.get('policy', 'RAISE').upper()],
    )

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
            on_error=error_policy,
        ),
    )


def jsonschema_to_yaml(jsonschema_file):
    """
    Converts a JSON Schema file to YAML format with:
    - Descriptions as comments.
    - Primitive types as placeholders (e.g., <string>, <integer>).
    - Enum values displayed as 'value1|value2|etc'.

    Args:
        jsonschema_file (str): Path to the JSON Schema file.
        yaml_output_file (str): Path to save the generated YAML file.
    """
    # Load the JSON schema
    with open(jsonschema_file, 'r') as file:
        main_schema = json.load(file)

    def process_properties(properties, level=0):
        """
        Process JSON Schema properties to generate YAML with comments.
        """
        yaml_output = []
        indent = "  " * level

        for key, value in properties.items():
            description = value.get("description", "")

            # Add description as a YAML comment
            if description:
                yaml_output.append(f"{indent}# {description}")

            # Determine placeholder value
            if "enum" in value:
                placeholder = " | ".join(map(str, value["enum"]))  # Enum values as a list
            elif "type" in value:
                type_mapping = {
                    "string": "<string>",
                    "integer": "<integer>",
                    "boolean": "<boolean>",
                    "number": "<number>",
                    "array": "<array>",
                    "object": "<object>"
                }
                placeholder = type_mapping.get(value["type"], "<unknown>")
            else:
                placeholder = "<unknown>"

            # Handle different schema types
            if value.get("type") == "object":
                yaml_output.append(f"{indent}{key}:")
                if "properties" in value:
                    yaml_output.extend(process_properties(value["properties"], level + 1))
            elif value.get("type") == "array":
                yaml_output.append(f"{indent}{key}:")
                if "items" in value and "properties" in value["items"]:
                    yaml_output.append(f"{indent}  -")  # Show array structure
                    yaml_output.extend(process_properties(value["items"]["properties"], level + 2))
                else:
                    yaml_output.append(f"{indent}  - {placeholder}")
            else:
                yaml_output.append(f"{indent}{key}: {placeholder}")

        return yaml_output

    # Convert the root properties
    yaml_lines = process_properties(main_schema.get("properties", {}))
    return yaml_lines
