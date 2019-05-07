from cranial.messaging import base
from cranial.messaging.adapters.zmq import send_string


class Notifier(base.Notifier):
    def send(self, address, message, endpoint=None, **kwargs):
        return send_string(message, address, wait=False)
