from cranial.messaging import base
import json


class Notifier(base.Notifier):
    @staticmethod
    def send(address=None, message='', endpoint=None, serde=json, **kwargs):
        label = []
        if address:
            label.append(address)
        if endpoint:
            label.append(endpoint)

        if type(message) not in [str, bytes]:
            message = serde.dumps(message)

        if len(label):
            print("{}: {}".format(', '.join(label), message))
        elif len(message):
            print(message[:-1] if message[-1] == '\n' else message)
