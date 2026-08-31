"""Engine seam for the functional test suite.

The integration tests assert on externally observable pipeline effects
(output topics, parquet files, iceberg tables), so they can run against
any engine implementing the sqlflow config spec. Select the engine with:

    SQLFLOW_ENGINE=python   (default) in-process Python sqlflow
    SQLFLOW_ENGINE=turbine  Go engine, invoked as a subprocess

For turbine, the config template is rendered here (Jinja2, same context
as the Python engine) and handed to the binary as plain YAML, so the
suite exercises the engine surface independently of the template layer.
The binary path comes from SQLFLOW_TURBINE_BIN (default: ./bin/turbine).
"""
import json
import os
import subprocess
import tempfile
from dataclasses import dataclass

import yaml

from sqlflow.config import render_config, new_from_path
from sqlflow.lifecycle import start


@dataclass
class EngineStats:
    num_messages_consumed: int = 0
    num_errors: int = 0


def engine():
    return os.environ.get('SQLFLOW_ENGINE', 'python')


def run_pipeline(config_path, setting_overrides=None, max_msgs=None):
    """Run a pipeline to completion and return its stats."""
    setting_overrides = setting_overrides or {}
    if engine() == 'turbine':
        return _run_turbine(config_path, setting_overrides, max_msgs)

    conf = new_from_path(config_path, setting_overrides)
    return start(conf, max_msgs=max_msgs)


def _run_turbine(config_path, setting_overrides, max_msgs):
    conf_dict = render_config(config_path, setting_overrides)

    binary = os.environ.get(
        'SQLFLOW_TURBINE_BIN',
        os.path.join(os.getcwd(), 'bin', 'turbine'),
    )

    with tempfile.NamedTemporaryFile(
            'w', suffix='.yml', prefix='turbine-conf-', delete=False) as f:
        yaml.safe_dump(conf_dict, f)
        rendered_path = f.name

    stats_path = rendered_path + '.stats.json'

    cmd = [binary, 'run', '-c', rendered_path, '--stats-json', stats_path]
    if max_msgs is not None:
        cmd += ['--max-msgs', str(max_msgs)]

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        raise RuntimeError(
            f'turbine exited {proc.returncode}\n'
            f'--- stdout ---\n{proc.stdout}\n'
            f'--- stderr ---\n{proc.stderr}'
        )

    with open(stats_path) as f:
        data = json.load(f)

    return EngineStats(
        num_messages_consumed=data.get('messages_consumed', 0),
        num_errors=data.get('num_errors', 0),
    )
