import io
import sys
import tempfile
import unittest

from cranial.connectors.local import Connector

sys.path.append('.')  # in case file is run from root dir


class TestLocalConnector(unittest.TestCase):
    def test_connector_init(self):
        c1 = Connector()
        c2 = Connector(path='temp_dir')
        actual = [c1.base_address, c2.base_address]
        expected = ['', 'temp_dir']
        self.assertEqual(actual, expected,
                         "should take path or use an empty string")

    def test_get(self):
        tf = tempfile.NamedTemporaryFile()
        expected = b'blah'
        tf.file.write(expected)  # type: ignore
        tf.file.flush()  # type: ignore

        c = Connector(path='/')
        actual = c.get(name=tf.name).read()
        tf.close()
        self.assertEqual(actual, expected, 'should read a temp file')

    def test_get_bad_name(self):
        # @TODO I don't remember why we thought this was a good idea.
        # We should probably raise most Exceptions.
        c = Connector(binary=False)
        actual = c.get(name='non-existing-file').read()
        self.assertEqual(actual, '', 'should return an empty string')

    def test_get_binary(self):
        tf = tempfile.NamedTemporaryFile(mode='rb')
        with open(tf.name, 'wb') as f:
            f.write(b'blah')

        c = Connector('/')
        actual = c.get(name=tf.name).read()
        expected = b'blah'
        tf.close()
        self.assertEqual(actual, expected, 'should read a temp file')

    def test_get_bad_name_binary(self):
        c = Connector(binary=True)
        actual = c.get(name='non-existing-file').read()
        expected = b''
        self.assertEqual(actual, expected, 'should return an empty string')

    def test_put_result(self):
        tfb = tempfile.NamedTemporaryFile(mode='rb')
        tf = tempfile.NamedTemporaryFile(mode='r')
        cb = Connector('/')
        c = Connector('/', binary=False)
        actual = [
            cb.put(io.BytesIO(b'blah'), name=tfb.name),
            cb.put(b'blah', name=tfb.name),
            cb.put(42, name=tfb.name),
            c.put(io.StringIO('blah'), name=tf.name),
            c.put('blah', name=tf.name),
            c.put(42, name=tf.name)
        ]
        expected = [True, True, False, True, True, False]
        tfb.close()
        tf.close()
        self.assertListEqual(
            actual, expected, 'should do two good writes and one failed')

    def test_put_check_written(self):
        tfb = tempfile.NamedTemporaryFile(mode='rb')
        tf = tempfile.NamedTemporaryFile(mode='r')
        cb = Connector('/')
        c = Connector('/', binary=False)

        _ = [
            cb.put(io.BytesIO(b'blah'), name=tfb.name),
            cb.put(b'blah', name=tfb.name),
            cb.put(42, name=tfb.name),
            c.put(io.StringIO('blah'), name=tf.name),
            c.put('blah', name=tf.name),
            c.put(42, name=tf.name)
        ]
        with open(tfb.name, 'rb') as f:
            resb = f.read()

        with open(tf.name, 'r') as f:  # type: ignore
            res = f.read()
        actual = [resb, res]
        expected = [b'blah', 'blah']
        tfb.close()
        tf.close()
        self.assertListEqual(actual, expected, 'should over-write blah')


if __name__ == '__main__':
    unittest.main()
