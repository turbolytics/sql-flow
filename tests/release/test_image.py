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
import contextlib
import json
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import duckdb
import pyarrow.dataset as ds
import pytest
import requests
from confluent_kafka import Producer
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType
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


@dataclass
class Stack:
    """The backing services the pipeline tests share.

    `bootstrap` is reachable from this test process; `internal` is the alias
    the container resolves. They are different addresses for the same broker.
    """
    network: Network
    bootstrap: str
    internal: str = "kafka:9092"


@pytest.fixture(scope="module")
def stack():
    """One broker and one network for the whole module.

    Each test used to start its own KafkaContainer. Four of them at 10-20
    seconds each dominated the suite, and nothing needed the isolation: every
    test derives its topics and group from the clock, so they cannot collide.
    """
    network = Network().create()
    kafka = KafkaContainer()
    kafka.with_network(network)
    kafka.with_network_aliases("kafka")
    kafka.start()
    try:
        yield Stack(network=network, bootstrap=kafka.get_bootstrap_server())
    finally:
        kafka.stop()
        # A test that started its own container on this network -- the
        # ClickHouse one does -- may still be attached, and Docker refuses to
        # remove a network with a live endpoint. Ryuk reaps it either way, so
        # a failure here must not fail the run: the tests have already passed
        # by the time this executes.
        try:
            network.remove()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask results
            print(f"leaving the test network for ryuk to reap: {exc}")


def unique(prefix):
    """A topic or group name no other test in this run will use."""
    return f"{prefix}-{int(time.time() * 1000)}"


@contextlib.contextmanager
def container_writable_dir():
    """A host directory the container writes into, cleaned up best-effort.

    The container runs as root, so the files and directories it creates are
    owned by root on the host. Docker Desktop maps that back to the calling
    user; a Linux CI runner does not, and removing the tree then fails with
    EPERM -- after the test has already passed.

    Cleanup is therefore best-effort. Leaking a directory under /tmp on a
    throwaway runner costs nothing; failing a green test on its teardown
    costs a red build and an hour working out that nothing was wrong.

    TemporaryDirectory(ignore_cleanup_errors=True) does NOT do this. Its
    handler chmods the parent on EPERM before it consults the flag, and that
    chmod raises EPERM of its own, uncaught. shutil.rmtree(ignore_errors=True)
    installs a handler that does nothing at all, so it cannot raise.
    """
    path = tempfile.mkdtemp()
    os.chmod(path, 0o777)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def run_pipeline(image, stack, config, env=None, volumes=None, max_msgs=None,
                 timeout=180, expect_exit=0):
    """Run one pipeline to completion in the image and return its stats.

    This is the seam the deleted Python integration suite had in engines.py.
    That suite ran ./bin/sqlflow directly; this runs the published image, so
    the entrypoint and the baked-in libduckdb are covered too.
    """
    with container_writable_dir() as statsdir:

        container = DockerContainer(image) \
            .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
            .with_volume_mapping(statsdir, "/stats", "rw") \
            .with_network(stack.network) \
            .with_env("SQLFLOW_KAFKA_BROKERS", stack.internal)

        # Every shipped config hardcoded `group_id: test`. One broker for the
        # whole module means every test would otherwise join that same group,
        # and a rebalance in one would move partitions out from under another.
        # The deleted integration suite worked around this by deleting the
        # group before each test; namespacing it removes the shared state
        # instead. A caller that needs a stable group across two runs -- the
        # restart test does, to resume from its committed offset -- passes one.
        env = dict(env or {})
        env.setdefault("SQLFLOW_GROUP_ID", unique(config.replace(".yml", "")))

        for key, value in env.items():
            container = container.with_env(key, str(value))
        for host_path, container_path in (volumes or {}).items():
            container = container.with_volume_mapping(host_path, container_path, "rw")

        command = (f"run /tmp/conf/config/examples/{config} "
                   f"--stats-json /stats/stats.json")
        if max_msgs is not None:
            command += f" --max-msgs={max_msgs}"
        container = container.with_command(command)

        container.start()
        try:
            wrapped = container.get_wrapped_container()
            code = wrapped.wait(timeout=timeout)["StatusCode"]
            logs = container.get_logs()
            assert code == expect_exit, (
                f"pipeline exited {code}, expected {expect_exit}\n"
                f"--- stdout ---\n{logs[0].decode()}\n"
                f"--- stderr ---\n{logs[1].decode()}"
            )
            with open(os.path.join(statsdir, "stats.json")) as fh:
                return json.load(fh)
        finally:
            container.stop()


def assert_all_messages_accounted_for(stats, published, rows, count_field):
    """Assert no input message was silently dropped.

    Consuming N messages is not evidence of processing them: a sink can drop
    rows, a batch can fail after its offsets commit, and a final partial batch
    can be lost at shutdown. These pipelines aggregate, so the row count alone
    cannot show completeness -- the per-group counts have to add back up to
    the number published.
    """
    assert stats["messages_consumed"] == published, (
        f'consumed {stats["messages_consumed"]}, published {published}')
    total = sum(row[count_field] for row in rows)
    assert total == published, (
        f"aggregate counts sum to {total}, published {published}")


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
    _, stdout, stderr = run_image(image, command)
    return stdout, stderr


# Exit codes from internal/errs/exit.go. Restated here on purpose: these are
# the contract a supervisor reads, so the release suite asserts the numbers
# rather than importing whatever the build happens to define.
EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_USER_ERROR = 10
EXIT_SOURCE_UNREACHABLE = 11
EXIT_SINK_UNREACHABLE = 12
EXIT_RESOURCE_LIMIT = 13
EXIT_STATE_CORRUPT = 14

# A supervisor must stop on these rather than restart into the same failure.
TERMINAL_EXITS = {EXIT_USER_ERROR, EXIT_STATE_CORRUPT}


def run_image(image, command, volumes=None, env=None):
    """Run the image and return (exit_code, stdout, stderr).

    The exit code is the point: it is what a supervisor reads to decide
    whether restarting can help, and run_docker_container drops it.
    """
    args = ["docker", "run", "--rm", "-v", f"{settings.DEV_DIR}:/tmp/conf"]
    for host, container in (volumes or {}).items():
        args += ["-v", f"{host}:{container}:rw"]
    for key, value in (env or {}).items():
        args += ["-e", f"{key}={value}"]
    args.append(image)

    result = subprocess.run(
        args + command.split(),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    return result.returncode, result.stdout, result.stderr


@pytest.mark.covers("cli.dev_invoke", "sink.console")
def test_handler_inferred_mem_invoke_renders_rows(image):
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


@pytest.mark.covers("config.templating")
def test_config_validation_accepts_a_shipped_example(image):
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

@pytest.mark.covers("source.kafka", "sink.kafka")
def test_handler_inferred_mem_aggregates_every_message(image, stack):
    num_messages = 1000
    in_topic = unique('input-simple-agg-mem')
    out_topic = unique('output-simple-agg-mem')

    KafkaFaker(
        bootstrap_servers=stack.bootstrap,
        num_messages=num_messages,
        topic=in_topic,
    ).publish()

    stats = run_pipeline(
        image, stack, "basic.agg.mem.yml",
        env={"SQLFLOW_INPUT_TOPIC": in_topic, "SQLFLOW_OUTPUT_TOPIC": out_topic},
        max_msgs=num_messages,
    )

    messages = read_all_kafka_messages(stack.bootstrap, out_topic)

    # One batch of 1000 messages aggregated to one row per city, published as
    # one Kafka message each. Their counts must add back up to every message
    # published, which a row count alone cannot show.
    assert 5 == len(messages), messages
    assert_all_messages_accounted_for(stats, num_messages, messages, 'city_count')



@pytest.mark.covers("source.websocket", "handler.structured")
def test_handler_inferred_mem_preserves_arrays_and_unioned_fields(image):
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


def test_sink_clickhouse_inserts_rows(image, stack, request):
    """A sink other than console/kafka, exercised through the image.

    Every sink silently no-oped at one point in this engine's history, and
    the ClickHouse sink could not write Array(T) at all until recently. No
    other image test writes to a database, so that whole class of failure
    ships unnoticed.
    """
    in_topic = unique("input-clickhouse-sink-user-actions")
    num_messages = 500

    network = stack.network

    clickhouse_ctr = DockerContainer("clickhouse/clickhouse-server:24.8-alpine")
    clickhouse_ctr.with_network(network)
    clickhouse_ctr.with_network_aliases("clickhouse")
    clickhouse_ctr.with_exposed_ports(8123)
    # Without this the image generates a random password for the default user,
    # and every query comes back 516 AUTHENTICATION_FAILED. The dev stack's
    # ClickHouse has no password, and this config's dsn carries no credentials.
    clickhouse_ctr.with_env("CLICKHOUSE_SKIP_USER_SETUP", "1")
    clickhouse_ctr.start()
    # Stopped explicitly: the network is now shared with the whole module, and
    # Docker will not remove a network that still has an endpoint attached.
    request.addfinalizer(clickhouse_ctr.stop)

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
    producer = Producer({"bootstrap.servers": stack.bootstrap})
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
        .with_env("SQLFLOW_INPUT_TOPIC", in_topic) \
        .with_env("SQLFLOW_GROUP_ID", in_topic) \
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


@pytest.mark.covers("state.offsets", "manager.tumbling_window", "source.kafka")
def test_state_durability_survives_a_restart(image, stack):
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

    network = stack.network
    producer = Producer({"bootstrap.servers": stack.bootstrap})
    # The current hour, so the window is still open for the length of the run
    # and recovery is observed on a partial aggregate. A fixed timestamp goes
    # stale: once it is more than an hour old the manager legitimately closes
    # the window, publishes it and deletes the rows these assertions read.
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    for i in range(num_messages):
        producer.produce(topic, json.dumps({
            "timestamp": timestamp,
            "properties": {"city": "NYC"},
            "user": {"id": str(i)},
        }))
    producer.flush()

    with container_writable_dir() as state_dir:
        # The container writes the state file here; both runs share it, and
        # the assertions below read it from the host afterwards.

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


def test_lifecycle_drain_writes_the_buffered_batch_on_sigterm(image, stack):
    """A supervisor stops a pipeline with SIGTERM, so SIGTERM must drain it.

    Go terminates on SIGTERM without running any deferred function. The process
    once died at once: exit 143, the buffered batch never written, the
    managers' final poll never run.

    The pipeline lost no data, because the offsets had not advanced either.
    Every graceful stop still threw away its tail and replayed it.

    This run publishes 300 messages against a batch size of 250, which leaves
    50 buffered. The flush interval outlasts the run, so only the drain can
    move them.
    """
    topic = f"sigterm-drain-{int(time.time())}"
    num_messages = 300
    batch_size = 250

    network = stack.network
    producer = Producer({"bootstrap.servers": stack.bootstrap})
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    for i in range(num_messages):
        producer.produce(topic, json.dumps({
            "timestamp": timestamp,
            "properties": {"city": "NYC"},
            "user": {"id": str(i)},
        }))
    producer.flush()

    with container_writable_dir() as state_dir:

        # No --max-msgs: the pipeline runs until a signal stops it. A pipeline
        # that exits on its own proves nothing here.
        container = DockerContainer(image) \
            .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
            .with_volume_mapping(state_dir, "/state", "rw") \
            .with_env("SQLFLOW_KAFKA_BROKERS", "kafka:9092") \
            .with_env("SQLFLOW_STATE_PATH", "/state/state.db") \
            .with_env("SQLFLOW_TOPIC", topic) \
            .with_env("SQLFLOW_GROUP_ID", topic) \
            .with_env("SQLFLOW_BATCH_SIZE", str(batch_size)) \
            .with_network(network) \
            .with_command("run /tmp/conf/config/examples/kafka.stateful.window.yml")
        container.start()

        # The pipeline has consumed every message, so 50 remain buffered and
        # wait for something to write them.
        wait_for_logs(container, f"messages_consumed.*{num_messages}", timeout=120)

        wrapped = container.get_wrapped_container()
        wrapped.kill(signal="SIGTERM")
        result = wrapped.wait(timeout=60)
        exit_code = result["StatusCode"]

        state_file = os.path.join(state_dir, "state.db")
        conn = duckdb.connect(state_file, read_only=True)
        total = conn.execute("SELECT sum(count) FROM agg_city_count").fetchone()[0]
        offset = conn.execute('SELECT max("offset") FROM sqlflow_offsets').fetchone()[0]
        conn.close()

    # Without a handler this exits 2, not the 143 an unhandled SIGTERM usually
    # produces. sqlflow runs as PID 1 here, and the kernel discards a signal
    # that PID 1 has no handler for. The Go runtime re-raises it, gets nowhere,
    # and falls back to exit(2). The same binary as an ordinary child exits 143.
    assert exit_code == 0, f"SIGTERM should exit cleanly, got {exit_code}"
    assert total == num_messages, (
        f"drain lost the buffered batch: {total} of {num_messages}")
    assert offset == num_messages - 1, (
        f"stored offset should be the last processed message: {offset}")


@pytest.mark.covers("state.corruption")
def test_lifecycle_exit_codes_carry_the_error_code(image):
    """A supervisor reads the process's exit status, so the image must set it.

    The mapping from error code to exit code is a Go unit test's job, and it
    is covered there. What only the image can show is that the status survives
    the entrypoint and PID 1 -- where exit codes have already surprised us
    once: an unhandled SIGTERM exits 2 here and 143 as an ordinary child.

    Two cases, one per class, rather than one per error: a user error and a
    damaged state file. Both must be terminal, because a supervisor that
    retries either loops forever.
    """
    with container_writable_dir() as statedir:
        state = os.path.join(statedir, "state.db")
        with open(state, "wb") as fh:
            fh.write(b"this is not a duckdb database")
        os.chmod(state, 0o666)
        before = open(state, "rb").read()

        cases = [
            ("missing config", EXIT_USER_ERROR, "user.config.not_found",
             dict(command="run /tmp/conf/config/nope.yml")),
            ("corrupt state file", EXIT_STATE_CORRUPT, "system.state.corrupt",
             dict(command="run /tmp/conf/config/examples/kafka.stateful.window.yml"
                          " --max-msgs=1",
                  volumes={statedir: "/state"},
                  env={"SQLFLOW_STATE_PATH": "/state/state.db",
                       "SQLFLOW_KAFKA_BROKERS": "kafka:9092"})),
        ]

        for name, want_exit, want_code, kwargs in cases:
            code, stdout, stderr = run_image(image, **kwargs)
            output = stdout + stderr

            assert code == want_exit, (
                f"{name}: expected exit {want_exit}, got {code}\n{output}")
            assert want_exit in TERMINAL_EXITS
            assert want_code in output, f"{name}: no error code in\n{output}"
            # Cobra prints the whole flag list for any error a command
            # returns, which buries the one line saying what failed.
            assert "Flags:" not in output, f"{name}: usage text buried the error"

        # A damaged state file is the only copy of the pipeline's positions.
        assert open(state, "rb").read() == before, "the damaged file was modified"


# --------------------------------------------------------------------------
# Pipelines ported from tests/integration
#
# That suite ran against the Python engine by default -- nothing ever set
# SQLFLOW_ENGINE=turbine -- so these seven scenarios had never been exercised
# against the Go engine end to end. They run against the image here, which is
# what users actually run.
# --------------------------------------------------------------------------


def test_sink_iceberg_writes_every_row(image, stack):
    """Kafka to an Iceberg table, read back through pyiceberg.

    The warehouse is mounted at the same absolute path inside the container as
    outside it. The catalog stores absolute file:// URIs, so a path that
    differs across the boundary produces metadata this process cannot follow.
    """
    num_messages = 5000
    catalog_name = unique("release_iceberg").replace("-", "_")
    table_name = "default.city_events"

    with container_writable_dir() as warehouse:

        catalog = SqlCatalog(catalog_name, **{
            "uri": f"sqlite:///{warehouse}/catalog.db",
            "warehouse": f"file://{warehouse}",
        })
        catalog.create_namespace("default")
        iceberg_table = catalog.create_table(table_name, schema=Schema(
            NestedField(field_id=1, name="timestamp",
                        field_type=TimestampType(), required=False),
            NestedField(field_id=2, name="city",
                        field_type=StringType(), required=False),
        ))

        topic = unique("release-iceberg")
        KafkaFaker(bootstrap_servers=stack.bootstrap,
                   num_messages=num_messages, topic=topic).publish()

        env_prefix = f"PYICEBERG_CATALOG__{catalog_name.upper()}__"
        stats = run_pipeline(
            image, stack, "kafka.mem.iceberg.yml",
            env={
                "SQLFLOW_CATALOG_NAME": catalog_name,
                "SQLFLOW_TABLE_NAME": table_name,
                "SQLFLOW_INPUT_TOPIC": topic,
                env_prefix + "URI": f"sqlite:///{warehouse}/catalog.db",
                env_prefix + "WAREHOUSE": f"file://{warehouse}",
            },
            volumes={warehouse: warehouse},
            max_msgs=num_messages,
        )

        assert stats["messages_consumed"] == num_messages
        iceberg_table.refresh()
        assert len(iceberg_table.scan().to_arrow()) == num_messages


def test_sink_parquet_writes_every_row(image, stack):
    """Kafka to parquet files on disk, read back with pyarrow."""
    num_messages = 2000
    topic = unique("release-parquet")

    KafkaFaker(bootstrap_servers=stack.bootstrap,
               num_messages=num_messages, topic=topic).publish()

    with container_writable_dir() as out_dir:

        stats = run_pipeline(
            image, stack, "local.parquet.sink.yml",
            env={
                "SQLFLOW_SINK_BASE_PATH": out_dir,
                "SQLFLOW_BATCH_SIZE": 1000,
                "SQLFLOW_INPUT_TOPIC": topic,
            },
            volumes={out_dir: out_dir},
            max_msgs=num_messages,
        )
        assert stats["messages_consumed"] == num_messages

        parquet_files = [f for f in os.listdir(out_dir) if f.endswith(".parquet")]
        assert len(parquet_files) == 2, f"expected 2 files, got {parquet_files}"

        table = ds.dataset(out_dir, format="parquet").to_table()
        total = sum(row["num_records"] for row in table.to_pylist())
        assert total == num_messages, f"parquet holds {total} of {num_messages}"


def test_error_ignore_drops_bad_records_and_keeps_going(image, stack):
    """A malformed record must not stop the pipeline under policy IGNORE.

    Five invalid messages and five valid ones. All ten are consumed; only the
    five valid ones reach the sink.
    """
    in_topic = unique("release-ignore-in")
    out_topic = unique("release-ignore-out")

    producer = Producer({"bootstrap.servers": stack.bootstrap})
    for _ in range(5):
        producer.produce(in_topic, value="invalid!")
    for _ in range(5):
        producer.produce(in_topic, value=json.dumps({"properties": {"city": "test"}}))
    producer.flush()

    stats = run_pipeline(
        image, stack, "basic.agg.mem.yml",
        env={
            "SQLFLOW_SOURCE_ERROR_POLICY": "ignore",
            "SQLFLOW_INPUT_TOPIC": in_topic,
            "SQLFLOW_OUTPUT_TOPIC": out_topic,
            "SQLFLOW_BATCH_SIZE": 1,
        },
        max_msgs=10,
    )

    assert stats["messages_consumed"] == 10
    assert len(read_all_kafka_messages(stack.bootstrap, out_topic)) == 5


def test_handler_inferred_mem_joins_against_a_csv(image, stack):
    """A join against a CSV read from disk.

    SQLFLOW_STATIC_ROOT points at the mounted dev/ directory, which is where
    the CSV lives.
    """
    num_messages = 1000
    in_topic = unique("release-csv-join-in")
    out_topic = unique("release-csv-join-out")

    KafkaFaker(bootstrap_servers=stack.bootstrap,
               num_messages=num_messages, topic=in_topic).publish()

    stats = run_pipeline(
        image, stack, "csv.mem.join.yml",
        env={
            "SQLFLOW_STATIC_ROOT": "/tmp/conf",
            "SQLFLOW_INPUT_TOPIC": in_topic,
            "SQLFLOW_OUTPUT_TOPIC": out_topic,
        },
        max_msgs=num_messages,
    )

    assert stats["messages_consumed"] == num_messages
    messages = read_all_kafka_messages(stack.bootstrap, out_topic)
    assert len(messages) == num_messages, f"joined {len(messages)} of {num_messages}"


def test_handler_inferred_mem_enriches_every_row(image, stack):
    """Per-record enrichment through the handler SQL."""
    num_messages = 1000
    in_topic = unique("release-enrich-in")
    out_topic = unique("release-enrich-out")

    KafkaFaker(bootstrap_servers=stack.bootstrap,
               num_messages=num_messages, topic=in_topic).publish()

    stats = run_pipeline(
        image, stack, "enrich.yml",
        env={"SQLFLOW_INPUT_TOPIC": in_topic, "SQLFLOW_OUTPUT_TOPIC": out_topic},
        max_msgs=num_messages,
    )

    assert stats["messages_consumed"] == num_messages
    messages = read_all_kafka_messages(stack.bootstrap, out_topic)
    assert len(messages) == num_messages, f"enriched {len(messages)} of {num_messages}"


def test_error_dlq_diverts_a_record_the_handler_cannot_parse(image, stack):
    """A malformed record must reach the DLQ, not vanish.

    This is the handler.write phase: the record never becomes a row.
    """
    in_topic = unique("release-dlq-write-in")
    dlq_topic = unique("release-dlq-write-dlq")

    producer = Producer({"bootstrap.servers": stack.bootstrap})
    producer.produce(in_topic, value=b"{!invalidJSON!")
    producer.flush()

    stats = run_pipeline(
        image, stack, "kafka.dlq.yml",
        env={
            "SQLFLOW_INPUT_TOPIC": in_topic,
            "SQLFLOW_DLQ_TOPIC": dlq_topic,
            "SQLFLOW_SOURCE_ERROR_POLICY": "dlq",
            "SQLFLOW_BATCH_SIZE": 1,
        },
        max_msgs=1,
    )

    assert stats["messages_consumed"] == 1
    assert stats["num_errors"] == 1

    dlq = read_all_kafka_messages(stack.bootstrap, dlq_topic)
    assert len(dlq) == 1, f"expected 1 DLQ record, got {len(dlq)}"
    record = dlq[0]
    # The wording of a parse failure is the engine's business; that it is
    # reported, against which message and which phase, is the contract.
    assert record["error"]
    assert record["message"] == "{!invalidJSON!"
    assert record["phase"] == "handler.write"
    assert record["timestamp"]


def test_error_dlq_diverts_a_batch_the_handler_cannot_query(image, stack):
    """A valid record the handler SQL cannot bind against still reaches the DLQ.

    This is the handler.invoke phase: the record parsed, and the query failed.
    """
    in_topic = unique("release-dlq-invoke-in")
    dlq_topic = unique("release-dlq-invoke-dlq")

    producer = Producer({"bootstrap.servers": stack.bootstrap})
    producer.produce(in_topic, value=b'{"valid": "json"}')
    producer.flush()

    stats = run_pipeline(
        image, stack, "kafka.dlq.yml",
        env={
            "SQLFLOW_INPUT_TOPIC": in_topic,
            "SQLFLOW_DLQ_TOPIC": dlq_topic,
            "SQLFLOW_SOURCE_ERROR_POLICY": "dlq",
            "SQLFLOW_BATCH_SIZE": 1,
        },
        max_msgs=1,
    )

    assert stats["messages_consumed"] == 1
    assert stats["num_errors"] == 1

    dlq = read_all_kafka_messages(stack.bootstrap, dlq_topic)
    assert len(dlq) == 1, f"expected 1 DLQ record, got {len(dlq)}"
    record = dlq[0]
    assert 'Binder Error: Referenced column "broken" not found' in record["error"]
    assert record["message"] == "Handler invocation failed"
    assert record["phase"] == "handler.invoke"
