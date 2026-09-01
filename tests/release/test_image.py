"""Functional tests against the released container image.

These exercise the shipped artifact rather than the source tree: the image is
what users run, and its entrypoint, baked-in libduckdb and CLI surface are
only really tested by running it.

The image under test comes from SQLFLOW_IMAGE, which `make test-image` sets to
the Go engine's tag. That engine is a drop-in replacement for the Python one --
same config spec, same entrypoint -- so these tests deliberately use the
Python engine's own CLI spelling (`run <config> --max-msgs-to-process`) to
prove an existing invocation keeps working against the new image.
"""

import ast
import json
import os
import subprocess

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.kafka import KafkaContainer

from sqlflow import settings
from sqlflow.fixtures import KafkaFaker
from sqlflow.kafka import read_all_kafka_messages


@pytest.fixture(scope="module")
def image():
    """The image under test.

    Defaults to the git-sha tag the legacy `make docker-image` produces, so a
    bare `pytest tests/release` still works the way it always did.
    """
    name = os.environ.get('SQLFLOW_IMAGE')
    if not name:
        git_sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"]).decode("utf-8").strip()
        assert git_sha, "Failed to get the Git SHA"
        name = f"turbolytics/sql-flow:{git_sha}"

    # Fail loudly rather than letting docker run report a pull failure: the
    # image is meant to have been built by the make target that got us here.
    proc = subprocess.run(["docker", "image", "inspect", name],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert proc.returncode == 0, (
        f"image {name} not present. Build it first (make sqlflow-image), or "
        f"set SQLFLOW_IMAGE to one that exists."
    )

    yield name


def parse_rows(stdout):
    """Read result rows from either engine's console rendering.

    The Go engine prints JSONL; the Python engine prints repr() of a list of
    dicts. Both are parsed here so one assertion covers whichever image is
    under test.
    """
    text = stdout.strip()
    if not text:
        return []
    if text.startswith('['):
        return ast.literal_eval(text)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


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


def test_sqlflow_docker_invoke_readme_example(image):
    """The README quickstart, run against the image.

    The Go engine's console sink emits one JSON object per line where the
    Python engine printed a Python list of dicts. Same rows, different
    rendering -- so this accepts either, and asserts on the rows themselves.
    """
    stdout, stderr = run_docker_container(
        image,
        "dev invoke /tmp/conf/config/examples/basic.agg.mem.yml /tmp/conf/fixtures/basic.agg.jsonl",
    )

    assert parse_rows(stdout) == [
        {'city': 'New York', 'city_count': 1},
        {'city': 'Baltimore', 'city_count': 1},
    ], f"unexpected output: {stdout!r} stderr={stderr!r}"


def test_sqlflow_docker_version(image):
    """The entrypoint resolves and the binary is stamped.

    A `dev` version means the build args never reached the -ldflags symbol
    path, which is silent otherwise.
    """
    stdout, _ = run_docker_container(image, "version")
    assert stdout.startswith("sqlflow "), stdout
    assert "\nsqlflow dev\n" not in "\n" + stdout, (
        f"image reports an unstamped version: {stdout!r}"
    )


def test_sqlflow_docker_config_validate(image):
    """config validate works with no libduckdb setup beyond the image."""
    stdout, stderr = run_docker_container(
        image, "config validate /tmp/conf/config/examples/basic.agg.mem.yml")
    assert "valid" in stdout, f"stdout={stdout!r} stderr={stderr!r}"

    # TODO: Figure out how to get the logs from the container using the testcontainers library
    # Testcontainers is way faster than going through subprocesses
    '''
    container = DockerContainer(image) \
        .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
        .with_command("dev invoke /tmp/conf/config/examples/basic.agg.mem.yml /tmp/conf/fixtures/simple.json")

    # result = container.exec("ls -la /tmp/conf")
    logs = container.get_logs()
    print(logs, container.get_logs())
    assert expected == container.get_logs()
    '''

def test_basic_agg_mem_readme_example(image):
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

    sqlflow = DockerContainer(image) \
        .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
        .with_env("SQLFLOW_KAFKA_BROKERS", 'kafka:9092') \
        .with_network(network) \
        .with_command("run /tmp/conf/config/examples/basic.agg.mem.yml  --max-msgs-to-process=1000")

    sqlflow.start()
    # Each engine words its own completion differently -- the Python engine
    # logs "consumer loop ending", the Go engine "max messages consumed" --
    # so this waits for whichever the image under test emits.
    wait_for_logs(
        sqlflow,
        "consumer loop ending|max messages consumed",
        timeout=60,
    )

    messages = read_all_kafka_messages(bootstrap_server, out_topic)

    # One batch of 1000 messages aggregated to one row per city, published as
    # one Kafka message each.
    assert 5 == len(messages), messages

