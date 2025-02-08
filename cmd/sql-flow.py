import json
import logging
import os
import threading

import click
import duckdb
from flask import Flask
from opentelemetry import metrics as otelmetrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import start_http_server
import jsonschema

from sqlflow import logging as sqlflow_logging, settings
from sqlflow.config import new_from_path, render_config, jsonschema_to_yaml
from sqlflow.http import DebugAPI
from sqlflow.lifecycle import start, invoke


logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@click.command(help='Start the SQLFlow service.')
@click.argument('config')
@click.option(
    '--max-msgs-to-process',
    type=int,
    default=None,
    help='Terminate execution after successfully processing this number.',
)
@click.option(
    '--with-http-debug',
    is_flag=True,
    help='Start Flask HTTP server for debugging.',
)
@click.option(
    '--metrics',
    type=click.Choice(['prometheus'], case_sensitive=False),
    default=None,
    help='Specify the metrics type.',
)
def run(config, max_msgs_to_process, with_http_debug, metrics):
    conf = new_from_path(config)
    conn = duckdb.connect()
    lock = threading.Lock()

    if metrics == 'prometheus':
        logger.info('Starting Prometheus metrics server: http://localhost:8000')
        metric_reader = PrometheusMetricReader()
        provider = MeterProvider(metric_readers=[metric_reader])
        otelmetrics.set_meter_provider(provider)
        start_http_server(port=8000)


    if with_http_debug:
        app = Flask(__name__)
        app.add_url_rule(
            '/debug',
            view_func=DebugAPI.as_view(
                'sql_query',
                conn=conn,
                lock=lock,
            ),
        )
        flask_thread = threading.Thread(target=lambda: app.run(debug=True, use_reloader=False))
        flask_thread.start()

    start(
        conf,
        conn=conn,
        lock=lock,
        max_msgs=max_msgs_to_process,
    )


@click.group(help='')
def config():
    pass


@click.command(name='example')
def config_example():
    schema_path = os.path.join(settings.PACKAGE_ROOT, 'static', 'schemas', 'config.json')
    example_yml = jsonschema_to_yaml(schema_path)
    print('\n'.join(example_yml))


@click.command(name='validate', help='Validate the configuration file.')
@click.argument('config')
def config_validate(config):
    schema_path = os.path.join(settings.PACKAGE_ROOT, 'static', 'schemas', 'config.json')
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    # load the file and render env vars
    config_dict = render_config(config)
    # validate against json schema
    jsonschema.validate(config_dict, schema)


@click.group(help='Development commands.')
def dev():
    pass


@click.command(name='invoke', help='execute a pipeline against a static file. Use for verifying pipeline logic.')
@click.argument('config')
@click.argument('fixture')
def dev_invoke(config, fixture):
    conn = duckdb.connect()
    invoke(conn, config, fixture)


config.add_command(config_validate)
config.add_command(config_example)
dev.add_command(dev_invoke)

cli.add_command(run)
cli.add_command(dev)
cli.add_command(config)


if __name__ == '__main__':
    sqlflow_logging.init()
    cli()
