import subprocess
import unittest

import pytest
from click import command
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.kafka import KafkaContainer

from sqlflow import settings
from sqlflow.fixtures import KafkaFaker
from sqlflow.kafka import read_all_kafka_messages
from tests.integration.test_integration import bootstrap_server


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
      "dev invoke /tmp/conf/config/examples/basic.agg.mem.yml /tmp/conf/fixtures/basic.agg.jsonl",
    )
    assert stdout.strip() == "[{'city': 'New York', 'city_count': 1}, {'city': 'Baltimore', 'city_count': 1}]"

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

def test_basic_agg_mem_readme_example(git_sha):
    num_messages = 1000
    in_topic = 'input-simple-agg-mem'
    out_topic = 'output-simple-agg-mem'

    network = Network().create()

    kafka_ctr = KafkaContainer()
    kafka_ctr.with_network(network)
    kafka_ctr.with_network_aliases("kafka")
    kafka_ctr.start()

    bootstrap_server = kafka_ctr.get_bootstrap_server()

    kf = KafkaFaker(
        bootstrap_servers=bootstrap_server,
        num_messages=num_messages,
        topic=in_topic,
    )
    kf.publish()

    sqlflow = DockerContainer(f"turbolytics/sql-flow:{git_sha}") \
        .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
        .with_env("SQLFLOW_KAFKA_BROKERS", 'kafka:9092') \
        .with_network(network) \
        .with_command("run /tmp/conf/config/examples/basic.agg.mem.yml  --max-msgs-to-process=1000")

    sqlflow.start()
    wait_for_logs(
        sqlflow,
        "consumer loop ending",
        timeout=10,
    )

    messages = read_all_kafka_messages(bootstrap_server, out_topic)
    assert 1000 == len(messages)

