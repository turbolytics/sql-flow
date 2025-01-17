import subprocess
import unittest

import pytest
from testcontainers.core.container import DockerContainer

from sqlflow import settings

@pytest.fixture(scope="module")
def git_sha():
    git_sha = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("utf-8").strip()
    print(git_sha)
    assert git_sha, "Failed to get the Git SHA"
    yield git_sha


def run_docker_container(image, command):
    result = subprocess.run(
        [
            "docker",
            "run",
            "-v",
            f"{settings.DEV_DIR}:/tmp/conf",
            "--rm",
            image,
        ] + command.split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    return result.stdout, result.stderr


def test_sqlflow_docker_invoke_readme_example(git_sha):
    expected = (b"[{'city': 'New York', 'city_count': 28672}, {'city': 'Baltimore', 'city_count': 28672}]\n", b'')
    stdout, stderr = run_docker_container(
        f"turbolytics/sql-flow:{git_sha}",
      "dev invoke /tmp/conf/config/examples/basic.agg.mem.yml /tmp/conf/fixtures/simple.json",
    )
    assert stdout.strip() == "[{'city': 'New York', 'city_count': 28672}, {'city': 'Baltimore', 'city_count': 28672}]"

    # TODO: Figure out how to get the logs from the container using the testcontainers library
    # Testcontainers is way faster than going through subprocesses
    '''
    container = DockerContainer(f"turbolytics/sql-flow:{git_sha}") \
        .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
        .with_command("dev invoke /tmp/conf/config/examples/basic.agg.mem.yml /tmp/conf/fixtures/simple.json")

    # result = container.exec("ls -la /tmp/conf")
    logs = container.get_logs()
    print(logs, container.get_logs())
    assert expected == container.get_logs()
    '''
