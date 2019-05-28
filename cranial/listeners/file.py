from cranial.listeners import base
from cranial.messaging.file import parts_to_path

from smart_open import open


class Listener(base.Listener):
    """Reads lines from a file.

    Using the strange parameter names provides configuration magic.
    """

    def __init__(self, address=None, endpoint=None, path=None, **kwargs):
        if not path:
            path = parts_to_path(address, endpoint)
        self.f = open(path, 'r')

    def recv(self, **kwargs):
        return next(self.f)
