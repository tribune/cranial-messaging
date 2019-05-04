import sys

from cranial.listeners import base


class Listener(base.Listener):
    """Reads a line from STDIN as a message."""
    def recv(self, **kwargs):
        return next(sys.stdin)
