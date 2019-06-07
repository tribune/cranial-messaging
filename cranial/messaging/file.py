import json
import os
from typing import Dict, IO  # noqa

from smart_open import open

from cranial.messaging import base
from cranial.common import logger

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
                # make sure the path exists
                d, _ = os.path.split(endpoint)
                if d != '':
                    os.makedirs(d, exist_ok=True)
                self.logfiles[endpoint] = open(endpoint,
                                               'ab' if append else 'wb')

            bytes_written = self.logfiles[endpoint].write(
                message + '\n'.encode('utf-8'))
            if bytes_written > 0:
                return message
            else:
                raise Exception("Couldn't write to destination.")
        except Exception as e:
            raise base.NotifyException(
                "{} || endpoint: {} || message: {}".format(
                    e, endpoint, message))

    def finish(self):
        [fh.flush for fh in self.logfiles.values() if not fh.closed]

    def __del__(self):
        for _, fh in self.logfiles.items():
            fh.close()
