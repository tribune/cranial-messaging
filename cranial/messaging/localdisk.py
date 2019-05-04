import os
from typing import Dict, IO  #noqa

from cranial.messaging import base


class Notifier(base.Notifier):
    """ Write messages to a local file named `address`

    Tested in LocalMessenger().
    """
    logfiles = {}  # type: Dict[str, IO]

    def send(self, address, message, endpoint=None):
        try:
            if address not in self.logfiles.keys() \
                    or self.logfiles[address].closed:
                # make sure the path exists
                d, _ = os.path.split(address)
                if d != '':
                    os.makedirs(d, exist_ok=True)
                self.logfiles[address] = open(endpoint, 'a')

            return self.logfiles[address].write(message + '\n')
        except Exception as e:
            raise base.NotifyException(
                "{} || address: {} || message: {}".format(
                    e, address, message))

    def finish(self):
        for _, fh in self.logfiles.items():
            fh.flush()

    def __del__(self):
        for _, fh in self.logfiles.items():
            fh.close()
