from cranial.messaging import base
from celery import Celery


class Notifier(base.Notifier):
    """ DEPRECATED. """

    def __init__(self, label='tasks', task_namespace='app.'):
        self.label = label
        self.namespace = task_namespace

    def send(self, address, message, endpoint, **kwargs):
        celery = Celery(self.label,
                        broker='pyamqp://guest@{}//'.format(address))
        celery.send_task(self.namespace + endpoint, (message,))
        return True
