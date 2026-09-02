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
import tempfile
import time

import duckdb
import pytest
import requests
from confluent_kafka import Producer
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



def test_sqlflow_docker_bluesky_data_fidelity(image):
    """Arrays, unioned struct fields and decoded JSON escapes, via the image.

    The other invoke case uses a flat scalar fixture, so it passes on an
    engine that cannot infer arrays, drops struct fields a later message
    introduces, or never decodes string escapes -- three bugs that shipped.
    This asserts on the data itself, against a shipped config.
    """
    stdout, stderr = run_docker_container(
        image,
        "dev invoke /tmp/conf/config/examples/bluesky/bluesky.raw.stdout.yml "
        "/tmp/conf/fixtures/bluesky.jsonl",
    )

    rows = [json.loads(line) for line in stdout.splitlines() if line.strip()]
    assert len(rows) == 4, f"stdout={stdout!r} stderr={stderr!r}"

    records = [r["commit"]["record"] for r in rows]

    # JSON arrays infer as lists rather than failing the batch.
    assert records[0]["langs"] == ["en", "ja"], records[0]
    # ...including a list of structs holding a list of its own.
    assert records[0]["facets"][0]["features"][0]["tag"] == "alpha", records[0]
    # An empty array stays an empty list, not null.
    assert records[2]["langs"] == [], records[2]

    # "reply" appears only in the last message. It is a column at all only
    # because struct fields are unioned across the batch.
    assert "reply" in records[3], records[3]
    assert records[3]["reply"]["parent"]["uri"].startswith("at://"), records[3]

    # JSON escapes arrive decoded: a real newline and a real e-acute, not the
    # backslash sequences that were being stored verbatim.
    assert records[3]["text"] == "line one\nline two at a café", (
        repr(records[3]["text"])
    )


def _wait_for_clickhouse(url, timeout=90):
    """Polls until ClickHouse answers a query.

    Connection errors are a server still starting and are retried. An HTTP
    response is the server talking, so a non-200 is a real answer -- bad auth,
    say -- and retrying it just burns the whole timeout before reporting what
    it already knew.
    """
    deadline = time.monotonic() + timeout
    last = "no attempt made"
    while time.monotonic() < deadline:
        try:
            resp = requests.post(url, data=b"SELECT 1", timeout=5)
            if resp.status_code == 200 and resp.text.strip() == "1":
                return
            raise AssertionError(
                f"clickhouse answered {resp.status_code}: {resp.text[:300]}")
        except requests.exceptions.RequestException as exc:
            last = repr(exc)
        time.sleep(1)
    raise AssertionError(
        f"clickhouse did not answer within {timeout}s; last attempt: {last}")


def test_clickhouse_sink(image):
    """A sink other than console/kafka, exercised through the image.

    Every sink silently no-oped at one point in this engine's history, and
    the ClickHouse sink could not write Array(T) at all until recently. No
    other image test writes to a database, so that whole class of failure
    ships unnoticed.
    """
    in_topic = "input-clickhouse-sink-user-actions"
    num_messages = 500

    network = Network().create()

    kafka_ctr = KafkaContainer()
    kafka_ctr.with_network(network)
    kafka_ctr.with_network_aliases("kafka")
    kafka_ctr.start()

    clickhouse_ctr = DockerContainer("clickhouse/clickhouse-server:24.8-alpine")
    clickhouse_ctr.with_network(network)
    clickhouse_ctr.with_network_aliases("clickhouse")
    clickhouse_ctr.with_exposed_ports(8123)
    # Without this the image generates a random password for the default user,
    # and every query comes back 516 AUTHENTICATION_FAILED. The dev stack's
    # ClickHouse has no password, and this config's dsn carries no credentials.
    clickhouse_ctr.with_env("CLICKHOUSE_SKIP_USER_SETUP", "1")
    clickhouse_ctr.start()

    ch_url = (
        f"http://{clickhouse_ctr.get_container_host_ip()}:"
        f"{clickhouse_ctr.get_exposed_port(8123)}"
    )
    # Polled rather than waiting on a log line: this image serves HTTP without
    # ever printing "Ready for connections" to stdout, so log-scraping just
    # times out. Answering a query is the condition that actually matters.
    _wait_for_clickhouse(ch_url)

    def query(sql):
        resp = requests.post(ch_url, data=sql.encode("utf-8"), timeout=30)
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text}"
        return resp.text.strip()

    # The DDL the dev stack ships for this example config.
    query("CREATE DATABASE IF NOT EXISTS test")
    query("""CREATE TABLE IF NOT EXISTS test.user_actions (
        timestamp DateTime,
        user_id UInt64,
        action String,
        browser String
    ) ENGINE = MergeTree() ORDER BY (timestamp, user_id)""")

    # KafkaFaker emits city events; this config's handler selects the
    # user_actions shape, so the messages are published directly.
    producer = Producer({"bootstrap.servers": kafka_ctr.get_bootstrap_server()})
    actions = ["click", "view", "purchase"]
    browsers = ["chrome", "firefox", "safari"]
    for i in range(num_messages):
        producer.produce(in_topic, json.dumps({
            "timestamp": "2026-09-01 12:00:00",
            "user_id": i,
            "action": actions[i % len(actions)],
            "browser": browsers[i % len(browsers)],
        }).encode("utf-8"))
    producer.flush()

    sqlflow = DockerContainer(image) \
        .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
        .with_env("SQLFLOW_KAFKA_BROKERS", "kafka:9092") \
        .with_env("SQLFLOW_CLICKHOUSE_DSN", "clickhouse://clickhouse:8123/test") \
        .with_network(network) \
        .with_command(
            "run /tmp/conf/config/examples/kafka.clickhouse.yml "
            f"--max-msgs-to-process={num_messages}")
    sqlflow.start()
    wait_for_logs(
        sqlflow,
        "consumer loop ending|max messages consumed",
        timeout=120,
    )

    # The rows reached ClickHouse, rather than the sink quietly discarding
    # them and the pipeline reporting success.
    assert query("SELECT count() FROM test.user_actions") == str(num_messages)
    # And the values are the ones that were published, not defaults.
    assert query(
        "SELECT count(DISTINCT action) FROM test.user_actions") == str(len(actions))


def test_window_state_survives_a_restart(image):
    """A crash mid-window must not lose the aggregate.

    Before durable state, 2,000 events folded into an open window were lost
    by killing the process: the offsets had already been committed, so the
    consumer group showed lag 0, a restart replayed nothing, and the
    aggregate stayed permanently short with no signal that anything was
    wrong. State and offsets now commit together, so a restart resumes from
    the durable position and completes the window.
    """
    topic = f"stateful-window-{int(time.time())}"
    num_messages = 2000

    network = Network().create()
    kafka_ctr = KafkaContainer()
    kafka_ctr.with_network(network)
    kafka_ctr.with_network_aliases("kafka")
    kafka_ctr.start()

    producer = Producer({"bootstrap.servers": kafka_ctr.get_bootstrap_server()})
    timestamp = "2026-09-02T12:00:00.000Z"
    for i in range(num_messages):
        producer.produce(topic, json.dumps({
            "timestamp": timestamp,
            "properties": {"city": "NYC"},
            "user": {"id": str(i)},
        }))
    producer.flush()

    with tempfile.TemporaryDirectory() as state_dir:
        # The container writes the state file here; both runs share it, and
        # the assertions below read it from the host afterwards.
        os.chmod(state_dir, 0o777)

        def run_half():
            container = DockerContainer(image) \
                .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
                .with_volume_mapping(state_dir, "/state", "rw") \
                .with_env("SQLFLOW_KAFKA_BROKERS", "kafka:9092") \
                .with_env("SQLFLOW_STATE_PATH", "/state/state.db") \
                .with_env("SQLFLOW_TOPIC", topic) \
                .with_env("SQLFLOW_GROUP_ID", topic) \
                .with_network(network) \
                .with_command(
                    "run /tmp/conf/config/examples/kafka.stateful.window.yml "
                    f"--max-msgs={num_messages // 2}")
            container.start()
            wait_for_logs(
                container,
                "consumer loop ending|max messages consumed",
                timeout=120,
            )
            return container

        # First half, then the process goes away with the window still open
        # and nothing published.
        run_half()

        state_file = os.path.join(state_dir, "state.db")
        assert os.path.exists(state_file), "the pipeline did not create its state file"

        conn = duckdb.connect(state_file, read_only=True)
        halfway = conn.execute("SELECT sum(count) FROM agg_city_count").fetchone()[0]
        conn.close()
        assert halfway == num_messages // 2, (
            f"first run should have aggregated {num_messages // 2}, got {halfway}")

        # Second half in a brand new process, reading the state left behind.
        run_half()

        conn = duckdb.connect(state_file, read_only=True)
        total = conn.execute("SELECT sum(count) FROM agg_city_count").fetchone()[0]
        offset = conn.execute('SELECT max("offset") FROM sqlflow_offsets').fetchone()[0]
        conn.close()

    # Every event is accounted for across the restart, and the durable
    # position is the last message actually processed.
    assert total == num_messages, (
        f"aggregate lost rows across the restart: {total} of {num_messages}")
    assert offset == num_messages - 1, (
        f"stored offset should be the last processed message: {offset}")
