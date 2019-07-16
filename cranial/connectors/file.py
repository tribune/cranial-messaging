import io
import os
from smart_open import open
import boto3

from cranial.connectors import base
from cranial.common import logger

log = logger.get(name='local_fetchers')  # streaming log


def file_readlines(fp, delete_after=False):
    """
    memory efficient iterator to read lines from a file (readlines() method reads whole file)
    Parameters
    ----------
    fp
        path to a decompressed file downloaded from s3 key
    delete_after
        delete file after it was read
    Returns
    -------
        generator of lines
    """
    for line in open(fp):
        if line:
            yield line
        else:
            break

    if delete_after:
        try:
            os.unlink(fp)
        except Exception as e:
            log.warning(e)


class Connector(base.Connector):
    def __init__(self, path='', binary=True, do_read=False, serde=None):
        self.serde = serde
        self.base_address = path
        self.binary = binary
        self.do_read = do_read
        self._open_files = []  # type: List[FileHandle]
        self.closed = False

    def get(self, name=None):
        if name is not None and name.startswith('/'):
            name = name[1:]
        filepath = self.base_address if name is None else os.path.join(self.base_address, name)
        try:
            # todo mode = 'rb' if self.binary else 'r'
            fh = open(filepath)
            self._open_files.append(fh)
            res = fh
            # todo get last modified for file?
            # self.metadata = {
            #     'LastModified': datetime.utcfromtimestamp(
            #         os.path.getmtime(filepath))}
            log.info("Opened \t{}".format(filepath))

        except Exception as e:
            log.error("{}\tbase_address={}\tname={}\tfilepath={}".format(
                e, self.base_address, name, filepath))
            res = io.BytesIO() if self.binary else io.StringIO()

        if self.do_read:
            res = res.read()

        return res

    def put(self, source, name: str = None, append=False) -> bool:

        filepath = self.base_address if name is None else os.path.join(self.base_address, name)

        if isinstance(source, io.IOBase):
            source = source.read()
        elif isinstance(source, (str, bytes)):
            pass
        else:
            log.error('Source should be either a string, bytes or a readable buffer, got {}'.format(type(source)))
            return False

        try:
            mode = 'ab' if append else 'wb'

            with open(filepath, mode) as f:
                f.write(source)

            log.info("wrote to \t{}".format(filepath))
            return True

        except Exception as e:
            log.error("{}\tbase_address={}\tname={}".format(e, self.base_address, name))
            return False

    # returns a bucket which contains the keys of all files in directory of base_address
    def get_dir_keys(self, bucket: str = None, prefix: str = None):
        print('base_address: ' + self.base_address + 'prefix: ' + prefix)
        try:
            if bucket is None:
                bucket = self.base_address.split('//')[1].split('/')[0]
                prefix = self.base_address.split('//')[1].split('/')[1:]
                if '.' in prefix[-1]:
                    del prefix[-1]
                prefix = '/'.join(prefix)
            s3 = boto3.client('s3')
            keys = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return keys['Contents']
        except Exception as e:
            log.error("{}\tbase_address={}\t".format(
                e, self.base_address))
        return False

    def __del__(self):
        [fh.close() for fh in self._open_files]

    def close(self):
        self.closed = True
        self.__del__()



if __name__ == "__main__":
    import doctest
    doctest.testmod()
