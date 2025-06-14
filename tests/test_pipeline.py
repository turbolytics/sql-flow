import unittest
from datetime import datetime, timezone
from json import JSONDecodeError
from unittest.mock import patch, MagicMock

from sqlflow import config, serde, errors
from sqlflow.handlers import InferredMemBatch
from sqlflow.pipeline import SQLFlow, PipelineErrorPolicies
from sqlflow.sinks import ConsoleSink, NoopSink
from sqlflow.sources import Source, Message


class StaticSource(Source):
    def __init__(self, messages):
        self.messages = iter(messages)

    def start(self):
        pass

    def stream(self):
        return self

    def __next__(self):
        return next(self.messages)

    def close(self):
        pass

    def commit(self):
        pass


class TestPipeline(unittest.TestCase):

    def test_pipeline_sink_error_raises_by_default(self):
        messages = [
            Message(
                value=b'{"time": "2021-01-01T00:00:00Z", "value": 1}',
            ),
        ]
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='SELECT CAST(time AS TIMESTAMP) as timestamp FROM batch',
            deserializer=serde.JSON(),
        )
        sink = ConsoleSink()

        pipeline = SQLFlow(
            source,
            handler,
            sink,
            batch_size=1,
        )
        with self.assertRaises(TypeError):
            stats = pipeline.consume_loop(max_msgs=1)

    def test_pipeline_source_error_shows_example_input(self):
        messages = [
            Message(
                value=b'{!invalidJSON!',
            ),
        ]
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='SELECT CAST(time AS TIMESTAMP) as timestamp FROM batch',
            deserializer=serde.JSON(),
        )

        pipeline = SQLFlow(
            source,
            handler,
            sink=NoopSink(),
            batch_size=1,
        )
        with self.assertRaises(JSONDecodeError):
            stats = pipeline.consume_loop(max_msgs=1)

    def test_liveness_counter(self):
        messages = [
            Message(value=b'{"time": "2021-01-01T00:00:00Z", "value": 1}'),
            None,
        ]
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='SELECT CAST(time AS TIMESTAMP) as timestamp FROM batch',
            deserializer=serde.JSON(),
        )
        sink = NoopSink()
        sink.flush = MagicMock()

        pipeline = SQLFlow(
            source,
            handler,
            sink,
            batch_size=2,
            flush_interval_seconds=1  # Set a short interval for testing
        )

        # this is quite clunky. This should be simplified with a status loop.
        # the status loop will run on an interval and flush if necessary.
        pipeline._liveness_time = MagicMock(
            return_value=datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        )

        with patch('sqlflow.pipeline.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            pipeline.consume_loop(max_msgs=2)

        # sink should contain single message
        sink.flush.assert_called_once()

    def test_pipeline_source_error_ignores(self):
        messages = [
            Message(
                value=b'{!invalidJSON!',
            ),
        ]
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='SELECT CAST(time AS TIMESTAMP) as timestamp FROM batch',
            deserializer=serde.JSON(),
        )

        pipeline = SQLFlow(
            source,
            handler,
            sink=NoopSink(),
            batch_size=1,
            error_policies=PipelineErrorPolicies(policy=errors.Policy.IGNORE),
        )

        stats = pipeline.consume_loop(max_msgs=1)

        self.assertEqual(stats.num_errors, 1)
        self.assertEqual(stats.num_messages_consumed, 1)