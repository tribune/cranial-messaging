from concurrent.futures import Future
import io
import sys
import time
import unittest

from cranial.connectors.base import Connector

sys.path.append('.')    # in case file is run from root dir


class DummyConnector(Connector):
    def __init__(self):
        pass

    def get(self, name=None):
        s = 'some string and name={}'.format(name)
        time.sleep(0.5)
        return io.BytesIO(s.encode())

    def put(self, stream, name=None):
        time.sleep(0.5)
        return isinstance(stream, io.BytesIO)


class TestConnector(unittest.TestCase):

    def test_connector_get(self):
        c = DummyConnector()
        actual = c.get(name='blah').read().decode()
        expected = 'some string and name=blah'
        self.assertEqual(
            actual,
            expected,
            "should return a pre-defined utf8-encoded string "
            "and name passed into the method")

    def test_connector_put(self):
        c = DummyConnector()
        actual = c.put(io.BytesIO(b'some string'), name='blah')
        self.assertTrue(
            actual,
            "should return True that input is an instance of io.BytesIO")

    def test_toStream(self):
        c = DummyConnector()
        actual = [
            isinstance(c.toStream('lala'), io.StringIO),
            isinstance(c.toStream(b'lala'), io.BytesIO),
        ]
        expected = [True, True]
        self.assertListEqual(
            actual, expected,
            "toStream should convert bytes into BytesIO and strings into"
            "StringIO")

    def test_getFuture(self):
        c = DummyConnector()
        f = c.getFuture(name='blah')
        actual = isinstance(f, Future)
        self.assertTrue(actual, "should return an instance of Future")

    def test_getFuture_result(self):
        c = DummyConnector()
        f = c.getFuture(name='blah')
        while not f.done():
            time.sleep(0.1)
        actual = f.result().read().decode()
        expected = 'some string and name=blah'
        self.assertEqual(
            actual, expected,
            "final result should be a pre-defined utf8-encoded string "
            "and name passed into the method")

    def test_putFuture(self):
        c = DummyConnector()
        f = c.putFuture(io.BytesIO(b'some string'), name='blah')
        actual = isinstance(f, Future)
        self.assertTrue(actual, "should return an instance of Future")

    def test_putFuture_result(self):
        c = DummyConnector()
        f = c.putFuture(io.BytesIO(b'some string'), name='blah')
        while not f.done():
            time.sleep(0.1)
        actual = f.result()
        self.assertTrue(actual, "result should be True")

    def test_getMultiple_no_block(self):
        c = DummyConnector()
        res = c.getMultiple({'a': 'a', 'b': 'b'}, blocking=False)
        actual = {k: isinstance(v, Future) for k, v in res.items()}
        expected = {'a': True, 'b': True}
        self.assertDictEqual(
            actual, expected, "should return a dictionary with futures")

    def test_getMultiple_block(self):
        c = DummyConnector()
        res = c.getMultiple({'a': 'a', 'b': 'b'}, blocking=True)
        actual = {k: v.read().decode() for k, v in res.items()}
        expected = {
            'a': 'some string and name=a',
            'b': 'some string and name=b'
        }
        self.assertDictEqual(
            actual, expected, "should return a dictionary with streams")

    def test_putMultiple_no_block(self):
        c = DummyConnector()
        res = c.putMultiple({
            'a': io.BytesIO(b'some string'),
            'b': ['some string', 'b']   # << two args
        }, blocking=False)

        actual = {k: isinstance(v, Future) for k, v in res.items()}
        expected = {'a': True, 'b': True}
        self.assertDictEqual(
            actual, expected, "should return a dictionary with futures")

    def test_putMultiple_block(self):
        c = DummyConnector()
        res = c.putMultiple({
            'a': io.BytesIO(b'some string'),
            'b': ['some string', 'b']   # << two args
        }, blocking=True)

        actual = res
        expected = {
            'a': True,
            'b': False
        }
        self.assertDictEqual(
            actual, expected, "should return a dictionary with results")


if __name__ == '__main__':
    unittest.main()
