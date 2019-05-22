import os
from typing import Dict, IO  #noqa

from cranial.messaging import base
from cranial.common import logger

log = logger.get()

def parts_to_path(address, endpoint):
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

    def send(self, address, message, endpoint, **kwargs):
        endpoint = parts_to_path(address, endpoint)
        log.debug('Writing to file: {}'.format(endpoint))
        if type(message) is str:
            message = message.encode('utf-8')
        try:
            if endpoint not in self.logfiles.keys() \
                    or self.logfiles[endpoint].closed:
                # make sure the path exists
                d, _ = os.path.split(endpoint)
                if d != '':
                    os.makedirs(d, exist_ok=True)
                self.logfiles[endpoint] = open(endpoint, 'ab')

            bytes_written = self.logfiles[endpoint].write(
                    message + '\n'.encode('utf-8'))
            return bytes_written > 0
        except Exception as e:
            raise base.NotifyException(
                "{} || endpoint: {} || message: {}".format(
                    e, endpoint, message))

    def finish(self):
        [fh.flush for fh in self.logfiles.values() if not fh.closed]

    def __del__(self):
        for _, fh in self.logfiles.items():
            fh.close()
