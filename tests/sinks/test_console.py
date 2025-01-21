import unittest
from unittest.mock import MagicMock, call

import pyarrow as pa

from sqlflow.sinks import ConsoleSink


class ConsoleWriterTestCase(unittest.TestCase):

    def test_write_single_val(self):
        table = pa.Table.from_pydict({"greeting": ["hello"]})
        mock_f = MagicMock()
        w = ConsoleSink(f=mock_f)

        w.write_table(table)
        w.flush()

        mock_f.write.assert_has_calls([
            call('{"greeting": "hello"}'),
            call('\n'),
        ])

    def test_write_multiple_vals(self):
        table = pa.Table.from_pydict({
            "greeting": ["hello", "hi", "hey"],
            "name": ["Alice", "Bob", "Charlie"]
        })
        mock_f = MagicMock()
        w = ConsoleSink(f=mock_f)

        w.write_table(table)
        w.flush()

        mock_f.write.assert_has_calls([
            call('{"greeting": "hello", "name": "Alice"}'),
            call('\n'),
            call('{"greeting": "hi", "name": "Bob"}'),
            call('\n'),
            call('{"greeting": "hey", "name": "Charlie"}'),
            call('\n'),
        ])
