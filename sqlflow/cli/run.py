from sqlflow import new_from_path

from sqlflow.sql import new_sqlflow_from_conf, init_tables


def start(config, max_msgs=None):
    conf = new_from_path(config)
    init_tables(conf.tables)
    sflow = new_sqlflow_from_conf(conf)
    sflow.consume_loop(max_msgs)
