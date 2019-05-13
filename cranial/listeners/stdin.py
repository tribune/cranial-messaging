import sys

from cranial.listeners import base


class Listener(base.Listener):
    """Reads a line from STDIN as a message."""
    def recv(self, **kwargs):
        # sys.stdin can be replaced with something like StringIO, which
        # doesn't have 'buffer.' Otherwise, using the buffer is necessary
        # to be able to handle binary data.
        return next(sys.stdin.buffer) if hasattr(sys.stdin, 'buffer') \
                else next(sys.stdin)
