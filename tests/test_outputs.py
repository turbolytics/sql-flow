import unittest
from unittest.mock import patch, call, MagicMock

from sqlflow.sinks import ConsoleSink, KafkaSink


class ConsoleWriterTestCase(unittest.TestCase):

    @patch('sys.stdout')
    def test_write_val_with_newline(self, mock_stdout):
        w = ConsoleSink()
        w.write('hello')

        mock_stdout.write.assert_has_calls([
            call('hello'),
            call('\n'),
        ])


class KafkaWriterTestCase(unittest.TestCase):
    def test_write_key_value(self):
        producer = MagicMock()
        w = KafkaSink(
            topic='test',
            producer=producer,
        )

        w.write(val='value', key='key')
        producer.produce.assert_called_once_with(
            'test',
            key='key',
            value=b'value',
        )

