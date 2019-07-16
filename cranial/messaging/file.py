import json
import os
from typing import Dict, IO, Optional

from cranial.messaging import base, MessageTypes
from cranial.common import logger
from cranial.connectors import FileConnector

log = logger.get()


class Notifier(base.Notifier):
    """ Write messages to a file named `endpoint`

    """
    files = {}  # type: Dict[str, IO]

    def send(self,
            address: Optional[str] = None,
            message: MessageTypes = '',
            endpoint: Optional[str] = None,
            append=False,
            path: Optional[str] = None
            **kwargs):
        if not ((address and endpoint) or path):
            raise Exception(
                'Must provide either path, or address and endpoint.')
        endpoint = path or self.parts_to_path(address, endpoint)
        log.debug('Writing to file: {}'.format(endpoint))
        if type(message) is str:
            message = message.encode('utf-8')
        elif type(message) != bytes:
            message = self.serde.dumps(message).encode('utf-8')
        try:
            if endpoint not in self.files.keys() \
                    or self.files[endpoint].closed:
                self.files[endpoint] = FileConnector(endpoint)

            success = self.files[endpoint].put(
                message + '\n'.encode('utf-8'), append=append)
            if success is True:
                return message
            else:
                raise Exception("Couldn't write to destination.")
        except Exception as e:
            raise base.NotifyException(
                "{} || endpoint: {} || message: {}".format(
                    e, endpoint, message))

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

    def get_last_id(self,  bucket: str = None,
                    prefix: str = None, serde=json, **kwargs):
        """ Takes an S3 endpoint and the seperator used in naming files
            gets all the files at the prefix in the given endpoint
            gets the most recently modified file with an id
            reads the last row from that file and returns that id as the
            last_id
        """

        if kwargs.get('address', '') is not '':
            endpoint = 's3://' + kwargs.get('address') + '/' + kwargs.get('endpoint')
        else:
            endpoint = kwargs.get('path', '')

        try:
            keys = FileConnector(endpoint).get_dir_keys(bucket=bucket, prefix=prefix)
            sorted_keys = sorted(keys, key=lambda item: item['LastModified'], reverse=True)

            adr = 's3://' + endpoint.split('//')[1].split('/')[0] + '/'
            last_file = FileConnector(adr + sorted_keys[0]['Key']).get()
            # todo default to json parsing if no parser is provided
            last_row = json.loads(last_file.read().split('\n')[-2])
            # todo dynamically tell what key id is under or pass it in
            last_id = list(last_row.values())[0]

        except Exception as e:
            raise base.NotifyException(
                "{} || endpoint: {}".format(
                    e, endpoint))

        return int(last_id)
