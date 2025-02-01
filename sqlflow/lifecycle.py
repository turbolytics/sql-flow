import threading

import duckdb

from sqlflow.config import new_from_path
from sqlflow import handlers, sinks
from sqlflow.pipeline import init_tables, build_managed_tables, handle_managed_tables, new_sqlflow_from_conf, init_udfs, \
    init_commands


def invoke(conn, config, fixture, setting_overrides={}, flush_window=False, invoke_sink=False):
    """
    Invoke will initialize config and invoke the configured pipleline against
    the provided fixture.

    :param conn:
    :param config:
    :param fixture:
    :param setting_overrides:
    :param flush_window: Flushes the managers after the invocation.
    :return:
    """
    conf = new_from_path(config, setting_overrides)

    init_commands(conn, conf.commands)
    init_tables(conn, conf.tables)
    init_udfs(conn, conf.udfs)

    h = handlers.new_handler_from_conf(
        conf.pipeline.handler,
        conn,
    )
    h.init()

    managed_tables = build_managed_tables(
        conn,
        conf.tables.sql,
    )

    if managed_tables:
        assert len(managed_tables) == 1, \
            "only a single managed table is currently supported"

    with open(fixture) as f:
        for line in f:
            cleaned_line = line.strip()
            if cleaned_line:
                h.write(cleaned_line)

    res = h.invoke()
    if flush_window:
        res = managed_tables[0].collect_closed()

    if invoke_sink:
        sink = sinks.new_sink_from_conf(conf.pipeline.sink, conn)
        sink.write_table(res)
        sink.flush()

    print(res.to_pylist())
    return res

async def start(conf, conn=None, lock=None, max_msgs=None):
    if conn is None:
        conn = duckdb.connect()

    if lock is None:
        lock = threading.Lock()

    h = handlers.new_handler_from_conf(
        conf.pipeline.handler,
        conn,
    )
    h.init()

    init_commands(conn, conf.commands)
    init_tables(conn, conf.tables)
    init_udfs(conn, conf.udfs)

    managed_tables = build_managed_tables(
        conn,
        conf.tables.sql,
        lock,
    )
    handle_managed_tables(managed_tables)

    sflow = new_sqlflow_from_conf(
        conf,
        conn,
        handler=h,
        lock=lock,
    )
    stats = await sflow.consume_loop(max_msgs)

    # flush and stop all managed tables
    for table in managed_tables:
        records = table.collect_closed()
        table.flush(records)
        table.stop()

    return stats
