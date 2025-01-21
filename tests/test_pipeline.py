import unittest

from sqlflow import config, serde
from sqlflow.handlers import InferredMemBatch
from sqlflow.pipeline import SQLFlow
from sqlflow.sinks import ConsoleSink
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

    def test_pipeline(self):
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
        '''
        self.assertEqual(len(sink.data), 1)
        self.assertIn('timestamp', sink.data[0])
        self.assertIsInstance(sink.data[0]['timestamp'], datetime.datetime)
        '''