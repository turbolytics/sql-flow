import unittest


class TestCase(unittest.TestCase):
    def test_fail(self):
        self.fail()