from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import glob
import io
import json
import os
from tempfile import mkstemp

from cranial.fetchers import connector
from cranial.common import logger

log = logger.get(name='local_fetchers')  # streaming log




class Connector(connector.Connector):
    def __init__(self, path: str = '', binary=True, do_read=False) -> None:
        super().__init__(base_address=path, binary=binary, do_read=do_read)
        self._open_files = []

    def get(self, name=None):
        if name.startswith('/'):
            name = name[1:]
        filepath = self.base_address if name is None else os.path.join(self.base_address, name)
        try:
            mode = 'rb' if self.binary else 'r'
            fh = open(filepath, mode)
            self._open_files.append(fh)
            res = fh
            self.metadata = {
                'LastModified': datetime.utcfromtimestamp(
                    os.path.getmtime(filepath))}
            log.info("Opened \t{}".format(filepath))

        except Exception as e:
            log.error("{}\tbase_address={}\tname={}\tfilepath={}".format(
                e, self.base_address, name, filepath))
            res = io.BytesIO() if self.binary else io.StringIO()

        if self.do_read:
            res = res.read()

        return res

    def put(self, source, name: str = None) -> bool:

        filepath = self.base_address if name is None else os.path.join(self.base_address, name)
        dir_path = os.path.split(filepath)[0]
        if len(dir_path) > 0:
            os.makedirs(dir_path, exist_ok=True)

        if isinstance(source, io.IOBase):
            source = source.read()
        elif isinstance(source, (str, bytes)):
            pass
        else:
            log.error('Source should be either a string, bytes or a readable buffer, got {}'.format(type(source)))
            return False

        try:
            mode = 'wb' if self.binary else 'w'

            # first write to a temp file
            _, local_path = self.get_tmp_file()
            with open(local_path, mode) as f:
                f.write(source)

            # then rename temp file to a proper name
            os.rename(local_path, filepath)
            log.info("wrote to \t{}".format(filepath))
            return True

        except Exception as e:
            log.error("{}\tbase_address={}\tname={}".format(e, self.base_address, name))
            return False

    def get_tmp_file(self):
        fd, local_path = mkstemp(dir=self.base_address)
        os.close(fd)
        return fd, local_path

    def __del__(self):
        [fh.close() for fh in self._open_files]


if __name__ == "__main__":
    import doctest
    doctest.testmod()
