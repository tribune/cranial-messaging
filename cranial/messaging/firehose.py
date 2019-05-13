from cranial.messaging.adapters import firehose
from cranial.messaging import base


class Notifier(base.Notifier):
    def send(self, address, message, endpoint, **kwargs):
        resp = firehose.put_data(endpoint, message)
        return resp
