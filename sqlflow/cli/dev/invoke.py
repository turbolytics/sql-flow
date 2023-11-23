from sqlflow.config import new_from_path


def invoke(config, fixture):
    conf = new_from_path(config)
    print(conf)