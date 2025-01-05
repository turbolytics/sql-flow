import threading

import click
import duckdb
from IPython.lib.deepreload import reload
from flask import Flask

from sqlflow import logging
from sqlflow.config import new_from_path
from sqlflow.http import DebugAPI
from sqlflow.lifecycle import start, invoke



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
def run(config, max_msgs_to_process, with_http_debug):
    conf = new_from_path(config)
    conn = duckdb.connect()
    lock = threading.Lock()

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
    logging.init()
    cli()
