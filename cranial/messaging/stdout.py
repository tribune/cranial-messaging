from cranial.messaging import base

class Notifier(base.Notifier):
    @staticmethod
    def send(self, address=None, message, endpoint=None):
        label = []
        if address:
            label.append(address)
        if endpoint:
            label.append(endpoint)
        print("{}: {}".format(', '.join(label), message))
