import subprocess
import unittest

import pytest
from testcontainers.core.container import DockerContainer

from sqlflow import settings

'''
@pytest.fixture(scope="module")
def git_sha():
    git_sha = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("utf-8").strip()
    print(git_sha)
    assert git_sha, "Failed to get the Git SHA"
    yield git_sha


@pytest.skip("Skip the test")
def test_sql_flow_docker():
    expected = (b"[{'city': 'New York', 'city_count': 28672}, {'city': 'Baltimore', 'city_count': 28672}]\n", b'')

    with DockerContainer(f"turbolytics/sql-flow:{git_sha}").with_volume_mapping(settings.DEV_DIR, "/tmp/conf").with_command(
        "dev invoke /tmp/conf/config/examples/basic.agg.mem.yml /tmp/conf/fixtures/simple.json"
    ) as container:
        logs = container.get_logs()
        assert expected == container.get_logs()
        print(logs)
'''
