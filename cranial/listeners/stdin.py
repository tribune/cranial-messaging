import ujson as json
import sys

from cranial.listeners import base


class Listener(base.Listener):
    """Reads a line from STDIN as a message."""

    def recv(self, **kwargs):
        # sys.stdin can be replaced with something like StringIO, which
        # doesn't have 'buffer.' Otherwise, using the buffer is necessary
        # to be able to handle binary data.
        message = next(sys.stdin.buffer) if hasattr(sys.stdin, 'buffer') \
            else next(sys.stdin)
        try:
            # Mypy doesn't know sys.stdin returns bytes.
            message = json.loads(message)  # type: ignore
        except:  # noqa
            pass
        return message
