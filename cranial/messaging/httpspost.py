from time import sleep
from typing import Union

import requests
import ujson as json

from cranial.messaging import base


class Notifier(base.Notifier):
    @staticmethod
    def send(address: str,
             message: Union[dict, str, bytes],
             endpoint: str,
             **kwargs):
        count = 0
        msgstr = str(message).strip()
        if type(message) is dict:
            data = json.dumps(message, ensure_ascii=False).encode('utf-8')
            headers = {'content-type': 'application/json; charset=utf-8'}
        else:
            data = msgstr
            headers = {}
        # Don't send empty messages.
        while count < 3 and msgstr != '':
            response = requests.post('https://{}/{}'.format(
                address, endpoint), data=data, headers=headers)
            if response.status_code == requests.codes.ok:
                try:
                    return response.json()
                except ValueError:
                    return response.text
            count += 1
            sleep(5)

        return False
