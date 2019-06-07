from time import sleep
from typing import Union

import requests
import ujson as json

from cranial.common import logger
from cranial.messaging import base

log = logger.get()


class Notifier(base.Notifier):
    @staticmethod
    def send(address: str,
             message: Union[dict, str, bytes],
             endpoint: str,
             headers: dict = None,
             **kwargs):
        count = 0
        msgstr = str(message).strip()
        headers = headers or {}
        if type(message) is dict:
            data = json.dumps(message, ensure_ascii=False).encode()
        else:
            data = msgstr.encode()

        if data.startswith(b'{"') and 'content-type' not in headers:
            headers['content-type'] = 'application/json; charset=utf-8'

        # Don't send empty messages.
        while count < 3 and msgstr != '':
            log.debug('Sending headers %s; Body: %s', headers, data)
            response = requests.post('https://{}/{}'.format(
                address, endpoint), data=data, headers=headers)
            if response.status_code == requests.codes.ok:
                try:
                    return response.json()
                except ValueError:
                    return response.text
            count += 1
            sleep(1)

        return False
