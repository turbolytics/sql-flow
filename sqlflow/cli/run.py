from sqlflow import new_from_path

from sqlflow.sql import new_sqlflow_from_conf


def start(config):
    conf = new_from_path(config)
    sflow = new_sqlflow_from_conf(conf)
    sflow.consume_loop()
