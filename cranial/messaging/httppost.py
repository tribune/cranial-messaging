from time import sleep

import requests

from cranial.messaging import base


class Notifier(base.Notifier):
    @staticmethod
    def send(address, message, endpoint, **kwargs):
        count = 0
        # Don't send empty messages.
        while count < 3 and message.strip() != '':
            response = requests.post('http://{}/{}'.format(
                address, endpoint), data=message)
            if response.status_code == requests.codes.ok:
                try:
                    return response.json()
                except ValueError:
                    return response.text
            count += 1
            sleep(5)

        return False
