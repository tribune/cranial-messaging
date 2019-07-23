import json
from typing import Any, Dict, IO, Optional  # noqa

from cranial.messaging import base, MessageTypes
from cranial.common import logger
from cranial.connectors.base import Connector  # noqa - Typing
from cranial.connectors import FileConnector

log = logger.get()


class Notifier(base.Notifier):
    """ Write messages to a file named `endpoint`
    """
    files = {}  # type: Dict[str, Connector]

    def send(self,
             address: str = '',
             message: MessageTypes = '',
             endpoint: str = '',
             append=False,
             path: Optional[str] = None,
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

    def parts_to_path(self, address: str, endpoint: str) -> str:
        """ Provides URI-based configurability, per cranial.common.config

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
