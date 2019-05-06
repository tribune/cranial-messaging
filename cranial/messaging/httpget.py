from time import sleep

import requests

from cranial.messaging import base


class Notifier(base.Notifier):
    @staticmethod
    def send(self, address, message, endpoint):
        count = 0
        while count < 3:
            # @TODO This should just POST message, instead of using the
            # third path part.
            response = requests.get('http://{}/{}/{}'.format(
                address, endpoint, message))
            if response.status_code == requests.codes.ok:
                return True
            count += 1
            sleep(5)

        return False
