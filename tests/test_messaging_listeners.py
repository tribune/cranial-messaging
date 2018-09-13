from unittest import TestCase

import socket
from contextlib import closing

from cranial.listeners.zmq import Listener
from cranial.messaging import Messenger


def random_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


class TestMessaging(TestCase):
    def test_zmq(self):
        port = random_port()
        listener = Listener(port)

        services = lambda _: {'all': {port: ['localhost:{}'.format(port)]}}
        protocols = lambda _: {'AsyncZMQ': services('NA')['all']}
        messenger = Messenger(endpoint='ignored',
                              label='NA',
                              service_discovery=services,
                              protocol_discovery=protocols)
        msg = 'Hello Test.'
        success = messenger.notify(msg)
        self.assertEqual(bytes(msg, 'ascii'), listener.recv())
        listener.resp(b'OK')
        self.assertTrue(success)
