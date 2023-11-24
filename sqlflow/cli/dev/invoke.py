import json

from sqlflow.sql import InferredBatch
from sqlflow.config import new_from_path


def invoke(config, fixture):
    conf = new_from_path(config)

    # load fixture into memory

    # only support JSON input right now
    # get from the config
    p = InferredBatch(conf)
    print(list(p.invoke(fixture)))