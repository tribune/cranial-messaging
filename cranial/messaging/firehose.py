from cranial.messaging.adapters import firehose
from cranial.messaging import base


class Notifier(base.Notifier):
    def __init__(self):
        self.client = firehose.get_client()

    def send(self, address, message, endpoint):
        self.firehose.put_record(DeliveryStreamName=endpoint,
                                 Record={'Data': bytes(message, 'utf8')})
        return True
