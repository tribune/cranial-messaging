from glob import iglob
import io
import os
from typing import List, IO, Iterator  # noqa

import boto3
from smart_open import open

from cranial.connectors import base
from cranial.common import logger

log = logger.get()


def file_readlines(fp):
    """
    Memory efficient iterator to read lines from a file
    (readlines() method reads whole file)

    Parameters
    ----------
    fp
        path to a file that can be opened by smart_open.
    Returns
    -------
        generator of lines
    """
    for line in open(fp):
        if line:
            yield line
        else:
            break


class Connector(base.Connector):
    def __init__(self, path='', binary=True, do_read=False):
        self.base_address = path
        self.binary = binary
        self.do_read = do_read
        self._open_files = []  # type: List[IO]

    def get(self, name=None):
        if name is not None and name.startswith('/'):
            name = name[1:]
        filepath = self.base_address if name is None \
            else os.path.join(self.base_address, name)
        try:
            mode = 'rb' if self.binary else 'r'
            res = open(filepath, mode)
            self._open_files.append(res)
            log.info("Opened \t{}".format(filepath))

        except Exception as e:
            log.error("{}\tbase_address={}\tname={}\tfilepath={}".format(
                e, self.base_address, name, filepath))
            raise e

        if self.do_read:
            res = res.read()

        return res

    def put(self, source, name: str = None, append=False) -> bool:
        filepath = self.base_address if name is None \
            else os.path.join(self.base_address, name)

        if '://' not in filepath:
            # We are writing a local file, so we need to make sure target
            # directories exist.
            dir_path = os.path.split(filepath)[0]
            if len(dir_path) > 0:
                os.makedirs(dir_path, exist_ok=True)

        if isinstance(source, (io.StringIO, io.BytesIO)):
            source = source.read()
        elif isinstance(source, (str, bytes)):
            pass
        else:
            raise Exception('Source should be either a string, bytes or a ' +
                            'readable buffer, got {}'.format(type(source)))

        try:
            mode = 'ab' if append else 'wb'

            # first write to  file
            with open(filepath, mode) as f:
                f.write(source)

            log.info("wrote to \t{}".format(filepath))
            return True

        except Exception as e:
            log.error("{}\tbase_address={}\tname={}".format(
                e, self.base_address, name))
            return False

    def list_names(self, prefix: str = '') -> Iterator[str]:
        """
        Lists all the items under a give filepath, for supported protocols.

        Test a listing in a  public S3 bucket and a local temp dir:
        >>> con = Connector('s3://landsat-pds/test/')
        >>> 'test.txt' in con.list_names()
        True
        >>> import tempfile
        >>> from pathlib import Path
        >>> from os.path import join
        >>> tmpdir = tempfile.TemporaryDirectory()
        >>> os.mkdir(join(tmpdir.name, 'pre'))
        >>> Path(join(tmpdir.name, 'top.file')).touch()
        >>> Path(join(tmpdir.name, 'pre', 'foo.file')).touch()
        >>> Path(join(tmpdir.name, 'pre', 'bar.file')).touch()
        >>> con = Connector(tmpdir.name)
        >>> [x for x in con.list_names('pre/')]
        ['pre/bar.file', 'pre/foo.file']
        >>> sorted([x for x in con.list_names()])
        ['pre/bar.file', 'pre/foo.file', 'top.file']
        """
        # Returns a List of paths.
        base_parts = self.base_address.split('//', 1)
        if len(base_parts) == 1:
            protocol = 'local'
            address = base_parts[0]
        else:
            protocol, address = base_parts

        if protocol.startswith('s3'):
            bucket, path = address.split('/', 1)
            s3 = boto3.client('s3')
            result = s3.list_objects_v2(Bucket=bucket, Prefix=path+prefix)
            contents = result.get('Contents')
            # @TODO We need to handle the case where there are more than 1000
            # keys, and so S3 requires another request to retrieve remaining
            # items. We can use the public 'irs-form-990' bucket to test this.
            for c in contents:
                name = c['Key'].replace(path, '')
                if name == '':
                    continue
                yield name
        elif protocol in ('local', 'file'):
            for x in iglob(
                    os.path.join(address, prefix, '**'),
                    recursive=True):
                if os.path.isdir(x):
                    continue
                name = x.replace(address + os.path.sep, '')
                yield name
        else:
            raise Exception('List_names is not implemented for %d.', protocol)

    def __del__(self):
        self.close()

    def close(self):
        [fh.close() for fh in self._open_files]


if __name__ == "__main__":
    import doctest
    doctest.testmod()
