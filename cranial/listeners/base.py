# Core modules.
from abc import ABCMeta, abstractmethod
import time

# 1st party
from cranial.common import logger

log = logger.create('listeners',
                    logger.fallback('LISTENERS_LOGLEVEL', 'WARNING'))


class ListenerHasNotResponded(Exception):
    pass


class Listener(metaclass=ABCMeta):
    """A Listener is a stateful object that listens for a request from a client
    and returns a response.

    - Users should not assume a Listener is thread-safe.

    - A Listener handles only one message at a time, so it can
    'remember' meta-data from the request to use in constructing the response.
    This means that if the consumer calls Listener.recv() twice before
    calling Listener.respond(), the first request may be aborted. (Individual
    implementations may choose how to handle this case.)

    For example, a single web server thread is an example of the concept of a
    Listener. One could trivially implement an HTTP listener.
    """

    def __init__(self, **kwargs):
        self.waiting = False

    @abstractmethod
    def recv(self) -> bytes:
        if self.waiting:
            raise ListenerHasNotResponded(
                'Called recv again before responding.')
        self.waiting = True
        return bytes()

    @abstractmethod
    def resp(self, data: bytes) -> bool:
        self.waiting = False
        successfully_sent = True
        return True if successfully_sent else False


class Demo(Listener):
    def __init__(self, events):
        self.events = events
        self.ix = 0
        super().__init__()

    def recv(self):
        super().recv()
        res = self.events[self.ix]
        res = '\t'.join([str(itm) for itm in res])
        res = res.encode('ascii')
        self.ix += 1
        if self.ix == len(self.events):
            self.ix = 0
        time.sleep(0.2)
        self.response = res
        return res

    def resp(self, data):
        super().resp(bytes(self.res))
