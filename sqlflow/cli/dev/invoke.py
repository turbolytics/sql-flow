from sqlflow.sql import init_tables
from sqlflow.handlers import InferredDiskBatch
from sqlflow.config import new_from_path


def invoke(config, fixture, setting_overrides={}):
    conf = new_from_path(config, setting_overrides)
    init_tables(conf.tables)
    p = InferredDiskBatch(conf).init()
    with open(fixture) as f:
        for line in f:
            p.write(line)
    res = list(p.invoke())
    print(res)
    return res
