import json
import os
from typing import Dict, IO  # noqa

from cranial.messaging import base
from cranial.common import logger
from cranial.connectors import FileConnector

log = logger.get()


def parts_to_path(address: str, endpoint: str) -> str:
    """ Provides URI string based configurability, per cranial.common.config

    file:///foo/bar is absolute;
    file://./foo/bar is relative.
    """
    if address in [None, '', 'localhost', '127.0.0.1', '/']:
        endpoint = '/' + endpoint
    elif address != '.':
        raise base.NotifyException("""Invalid address.
        If you intend to provide a relative filepath, use: file://./{}
        If you intend to write to another host, the 'file' Notifier does
        not yet support that. Try another protocol.
        """.format(endpoint))
    return endpoint


class Notifier(base.Notifier):
    """ Write messages to a local file named `endpoint`

    Tested in LocalMessenger().
    """
    logfiles = {}  # type: Dict[str, IO]

    def send(self, address=None, message='', endpoint=None, serde=json,
             append=False,
             **kwargs):
        if not ((address and endpoint) or kwargs.get('path')):
            raise Exception(
                'Must provide either path, or address and endpoint.')
        endpoint = kwargs.get('path') or parts_to_path(address, endpoint)
        log.debug('Writing to file: {}'.format(endpoint))
        if type(message) is str:
            message = message.encode('utf-8')
        elif type(message) != bytes:
            message = serde.dumps(message).encode('utf-8')
        try:
            if endpoint not in self.logfiles.keys() \
                    or self.logfiles[endpoint].closed:
                d, _ = os.path.split(endpoint)
                # make sure the path exists for actual local files.
                if d != '' and '://' not in endpoint:
                    os.makedirs(d, exist_ok=True)
                # todo pass to Instantiate File Connector with address and call get
                #   with endpoint.
                #   If given a path, break it up into address and endpoint
                # self.logfiles[endpoint] = open(endpoint, 'ab' if append else 'wb')
                self.logfiles[endpoint] = FileConnector(endpoint)

            # todo instead of write use put and pass in path
            bytes_written = self.logfiles[endpoint].put(
                message + '\n'.encode('utf-8'), append=append)
            if bytes_written is True:
                return message
            else:
                raise Exception("Couldn't write to destination.")
        except Exception as e:
            raise base.NotifyException(
                "{} || endpoint: {} || message: {}".format(
                    e, endpoint, message))

    def get_last_id(self, endpoint: str = '', sep: str = '', bucket: str = None,
                    prefix: str = None):
        """ Takes an S3 endpoint and the seperator used in naming files
            gets all the files at the prefix in the given endpoint
            gets the most recently modified file with an id
            reads the last row from that file and returns that id as the
            last_id
        """
        if sep == '':
            raise Exception("Need seperator to parse last_id from file names")

        # todo try to use FileConnector to get all keys then sort by datetime
        #   then return key. Actually use reduce
        # from config get endpoint
        # endpoint = params.get('endpoint', params.get('path', ''))

        # get all keys in bucket with connector
        keys = FileConnector(endpoint).get_dir_keys(bucket=bucket, prefix=prefix)
        # sort by last modified
        sorted_keys = sorted(keys, key=lambda item: item['LastModified'], reverse=True)
        # for each item tey to get the last id by
        #   splitting by sep and getting the last item in array
        #   if number we good
        last_file_key = ''
        for key in sorted_keys:
            if key['Key'].split(sep)[-1].split('.')[0].isdigit():
                last_file_key = key['Key']
                break
        #   read in that file get then get last id in the file
        if last_file_key is not '':
            base = 's3://' + endpoint.split('//')[1].split('/')[0] + '/'
            last_file = FileConnector(base + last_file_key).get().read()
            last_row = json.loads(last_file.split('\n')[-2])
            # todo dynmically tell what key id is under or pass it in
            # last_id = last_row['']
            last_id = list(last_row.values())[0]
        return last_id


    def finish(self):
        [fh.flush for fh in self.logfiles.values() if not fh.closed]

    def __del__(self):
        for _, fh in self.logfiles.items():
            fh.close()
