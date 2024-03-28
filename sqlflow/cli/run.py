from sqlflow import new_from_path
from sqlflow.handlers import get_class
from sqlflow.sql import new_sqlflow_from_conf, init_tables


def start(config, max_msgs=None):
    conf = new_from_path(config)
    BatchHandler = get_class(conf.pipeline.type)
    h = BatchHandler(conf)
    init_tables(conf.tables)
    sflow = new_sqlflow_from_conf(
        conf,
        handler=h,
    )
    sflow.consume_loop(max_msgs)
