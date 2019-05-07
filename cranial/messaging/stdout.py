from cranial.messaging import base


class Notifier(base.Notifier):
    @staticmethod
    def send(address=None, message='', endpoint=None, **kwargs):
        label = []
        if address:
            label.append(address)
        if endpoint:
            label.append(endpoint)

        if len(label):
            print("{}: {}".format(', '.join(label), message))
        else:
            print(message[:-1] if message[-1] == '\n' else message)
