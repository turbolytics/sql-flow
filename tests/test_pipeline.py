import unittest
from datetime import datetime, timezone
from json import JSONDecodeError
from unittest.mock import patch, MagicMock

from sqlflow import config, serde, errors
from sqlflow.handlers import InferredMemBatch
from sqlflow.pipeline import SQLFlow, PipelineErrorPolicy
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

    def test_check_liveness_called_once(self):
        messages = [None]  # Simulate no new messages to trigger liveness
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='SELECT * FROM batch',
            deserializer=serde.JSON(),
        )
        sink = NoopSink()

        pipeline = SQLFlow(
            source=source,
            handler=handler,
            sink=sink,
            batch_size=1,
            flush_interval_seconds=1,
        )

        with patch.object(pipeline, '_check_liveness') as mock_check_liveness:
            pipeline.consume_loop(max_msgs=1)

        mock_check_liveness.assert_called_once()

    def test_liveness_flush_triggers_sink_flush(self):
        messages = [
            Message(value=b'{"time": "2021-01-01T00:00:00Z", "value": 1}'),
            None  # Triggers liveness flush
        ]
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='SELECT * FROM batch',
            deserializer=serde.JSON(),
        )
        sink = NoopSink()
        sink.flush = MagicMock()

        pipeline = SQLFlow(
            source=source,
            handler=handler,
            sink=sink,
            batch_size=2,  # > 1 so liveness is needed
            flush_interval_seconds=1
        )

        with patch.object(pipeline, '_check_liveness') as mock_check_liveness:
            pipeline.consume_loop(max_msgs=2)

        mock_check_liveness.assert_called_once()
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
            error_policies=PipelineErrorPolicy(policy=errors.Policy.IGNORE),
        )

        stats = pipeline.consume_loop(max_msgs=1)

        self.assertEqual(stats.num_errors, 1)
        self.assertEqual(stats.num_messages_consumed, 1)

    def test_pipeline_handler_invoke_ignores_invalid_sql(self):
        messages = [
            Message(
                value=b'{"time": "2021-01-01T00:00:00Z", "value": 1}',
            ),
        ]
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='INVALID SQL SYNTAX',
            deserializer=serde.JSON(),
        )

        pipeline = SQLFlow(
            source,
            handler,
            sink=NoopSink(),
            batch_size=1,
            error_policies=PipelineErrorPolicy(policy=errors.Policy.IGNORE),
        )

        stats = pipeline.consume_loop(max_msgs=1)

        self.assertEqual(stats.num_errors, 1)
        self.assertEqual(stats.num_messages_consumed, 1)

    def test_pipeline_dlq_policy_invalid_json(self):
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
        dlq_sink = MagicMock()
        dlq_sink.write_table = MagicMock()

        pipeline = SQLFlow(
            source,
            handler,
            sink=NoopSink(),
            batch_size=1,
            error_policies=PipelineErrorPolicy(
                policy=errors.Policy.DLQ,
                dlq_sink=dlq_sink,
            ),
        )

        stats = pipeline.consume_loop(max_msgs=1)

        self.assertEqual(
            stats.num_errors,
            1,
            f'expected 1 error received: {stats.num_errors}',
        )
        self.assertEqual(
            stats.num_messages_consumed,
            1,
            f'expected 1 message consumed, received: {stats.num_messages_consumed}'
        )
        dlq_sink.write_table.assert_called_once()

    def test_pipeline_dlq_policy_invalid_sql(self):
        messages = [
            Message(
                value=b'{"time": "2021-01-01T00:00:00Z", "value": 1}',
            ),
        ]
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='INVALID SQL SYNTAX',
            deserializer=serde.JSON(),
        )
        dlq_sink = MagicMock()
        dlq_sink.write_table = MagicMock()

        pipeline = SQLFlow(
            source,
            handler,
            sink=NoopSink(),
            batch_size=1,
            error_policies=PipelineErrorPolicy(
                policy=errors.Policy.DLQ,
                dlq_sink=dlq_sink,
            ),
        )

        stats = pipeline.consume_loop(max_msgs=1)

        self.assertEqual(stats.num_errors, 1)
        self.assertEqual(stats.num_messages_consumed, 1)
        dlq_sink.write_table.assert_called_once()

    def test_pipeline_batch_size_handling(self):
        messages = [
            Message(value=b'{"time": "2021-01-01T00:00:00Z", "value": 1}'),
            Message(value=b'{"time": "2021-01-01T00:01:00Z", "value": 2}'),
        ]
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='SELECT CAST(time AS TIMESTAMP) as timestamp FROM batch',
            deserializer=serde.JSON(),
        )
        sink = MagicMock()
        sink.write_table = MagicMock()

        pipeline = SQLFlow(
            source,
            handler,
            sink,
            batch_size=2,
        )

        stats = pipeline.consume_loop(max_msgs=2)

        self.assertEqual(stats.num_messages_consumed, 2)
        sink.write_table.assert_called_once()

    def test_pipeline_source_commit_behavior(self):
        messages = [
            Message(value=b'{"time": "2021-01-01T00:00:00Z", "value": 1}'),
        ]
        source = StaticSource(messages)
        source.commit = MagicMock()
        handler = InferredMemBatch(
            sql='SELECT CAST(time AS TIMESTAMP) as timestamp FROM batch',
            deserializer=serde.JSON(),
        )
        sink = NoopSink()

        pipeline = SQLFlow(
            source,
            handler,
            sink,
            batch_size=1,
        )

        pipeline.consume_loop(max_msgs=1)

        source.commit.assert_called_once()

    def test_pipeline_sink_flush_behavior(self):
        messages = [
            Message(value=b'{"time": "2021-01-01T00:00:00Z", "value": 1}'),
        ]
        source = StaticSource(messages)
        handler = InferredMemBatch(
            sql='SELECT CAST(time AS TIMESTAMP) as timestamp FROM batch',
            deserializer=serde.JSON(),
        )
        sink = MagicMock()
        sink.flush = MagicMock()

        pipeline = SQLFlow(
            source,
            handler,
            sink,
            batch_size=1,
        )

        pipeline.consume_loop(max_msgs=1)

        sink.flush.assert_called_once()