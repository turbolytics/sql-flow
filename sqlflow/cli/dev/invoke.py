import json

from sqlflow.sql import InferredBatch, init_tables
from sqlflow.config import new_from_path


def invoke(config, fixture):
    conf = new_from_path(config)
    init_tables(conf.tables)
    p = InferredBatch(conf)
    print(list(p.invoke(fixture)))
