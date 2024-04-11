import duckdb

from sqlflow import new_from_path
from sqlflow.handlers import get_class
from sqlflow.serde import JSON
from sqlflow.sql import new_sqlflow_from_conf, init_tables


def start(config, max_msgs=None):
    conn = duckdb.connect()
    conf = new_from_path(config)

    BatchHandler = get_class(conf.pipeline.type)
    h = BatchHandler(
        conf,
        deserializer=JSON(),
        conn=conn,
    )

    init_tables(conn, conf.tables)

    sflow = new_sqlflow_from_conf(
        conf,
        conn,
        handler=h,
    )
    sflow.consume_loop(max_msgs)
