import asyncio
import logging
import threading

import click
import duckdb
from flask import Flask
from opentelemetry import metrics as otelmetrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import start_http_server

from sqlflow import logging as sqlflow_logging
from sqlflow.config import new_from_path
from sqlflow.http import DebugAPI
from sqlflow.lifecycle import start, invoke


logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@click.command()
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

    asyncio.run(start(
        conf,
        conn=conn,
        lock=lock,
        max_msgs=max_msgs_to_process,
    ))


@click.group()
def dev():
    pass


@click.command(name='invoke')
@click.argument('config')
@click.argument('fixture')
def cli_invoke(config, fixture):
    conn = duckdb.connect()
    invoke(conn, config, fixture)


dev.add_command(cli_invoke)
cli.add_command(run)
cli.add_command(dev)


if __name__ == '__main__':
    sqlflow_logging.init()
    cli()
