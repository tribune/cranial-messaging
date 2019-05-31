from cranial.messaging import base
import ujson as json


class Notifier(base.Notifier):
    @staticmethod
    def send(address=None, message='', endpoint=None,
             serde=json, serde_args=None, **kwargs):
        serde_args = serde_args or {}
        label = []
        if address:
            label.append(address)
        if endpoint:
            label.append(endpoint)

        if type(message) not in [str, bytes]:
            if serde == json:
                serde_args['ensure_ascii'] = False
            message = serde.dumps(message, **serde_args)

        if len(label):
            print("{}: {}".format(', '.join(label), message))
        elif len(message):
            print(message[:-1] if message[-1] == '\n' else message)
