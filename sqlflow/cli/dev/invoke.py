import json

from sqlflow.sql import InferredBatch, init_tables
from sqlflow.config import new_from_path


def invoke(config, fixture, setting_overrides={}):
    conf = new_from_path(config, setting_overrides)
    init_tables(conf.tables)
    p = InferredBatch(conf)
    res = list(p.invoke(fixture))
    print(res)
    return res
