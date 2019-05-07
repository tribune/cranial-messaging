import os
from typing import Dict, IO  #noqa

from cranial.messaging import base


def parts_to_path(address, endpoint):
    """ Provides URI string based configurability, per cranial.common.config 

    file:///foo/bar is absolute;
    file://./foo/bar is relative.
    """
    if address == '' or address is None:
        endpoint = '/' + endpoint
    elif address != '.':
        raise base.NotifyException("""Invalid address.
        If you intend to provide a relative filepath, use: file://./{}
        If you intend to write to another host, the 'file' Notifier does
        not support that. Try another protocol.
        """.format(endpoint))
    return endpoint


class Notifier(base.Notifier):
    """ Write messages to a local file named `endpoint`

    Tested in LocalMessenger().
    """
    logfiles = {}  # type: Dict[str, IO]

    def send(self, address, message, endpoint, **kwargs):
        endpoint = parts_to_path(address, endpoint)
        try:
            if endpoint not in self.logfiles.keys() \
                    or self.logfiles[endpoint].closed:
                # make sure the path exists
                d, _ = os.path.split(endpoint)
                if d != '':
                    os.makedirs(d, exist_ok=True)
                self.logfiles[endpoint] = open(endpoint, 'a')

            return self.logfiles[endpoint].write(message + '\n')
        except Exception as e:
            raise base.NotifyException(
                "{} || endpoint: {} || message: {}".format(
                    e, endpoint, message))

    def finish(self):
        for _, fh in self.logfiles.items():
            fh.flush()

    def __del__(self):
        for _, fh in self.logfiles.items():
            fh.close()
