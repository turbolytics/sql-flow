from sqlflow.serde import JSON
from sqlflow.sql import init_tables, build_managed_tables
from sqlflow.handlers import InferredDiskBatch, get_class
from sqlflow.config import new_from_path


def invoke(conn, config, fixture, setting_overrides={}, flush_window=False):
    """
    Invoke will initialize config and invoke the configured pipleline against
    the provided fixture.

    :param conn:
    :param config:
    :param fixture:
    :param setting_overrides:
    :param flush_window: Flushes the window after the invocation.
    :return:
    """
    conf = new_from_path(config, setting_overrides)

    BatchHandler = get_class(conf.pipeline.type)
    h = BatchHandler(
        conf,
        deserializer=JSON(),
        conn=conn,
    ).init()

    init_tables(conn, conf.tables)
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

    res = list(h.invoke())
    if not flush_window:
        print(res)
        return res

    res = managed_tables[0].collect_closed()
    print(res)

    return res
