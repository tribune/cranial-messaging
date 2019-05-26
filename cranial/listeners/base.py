# Core modules.
from abc import ABCMeta, abstractmethod
import time


class ListenerHasNotResponded(Exception):
    pass


class Listener(metaclass=ABCMeta):
    """A Listener is a stateful object that listens for a request from a client
    and returns a response.

    - Users should not assume a Listener is thread-safe; it depends on the
      implementation details. It's recommended to use a single Listener that
      recieves mesages and puts them into a thread-safe Queue for use by other
      threads.
    """
    def __init__(self, **kwargs):
        return

    @abstractmethod
    def recv(self, **kwargs) -> bytes:
        return bytes()


class RespondingListener(Listener, metaclass=ABCMeta):
    """A RespondingListener handles only one message at a time, so it can
    'remember' meta-data from the request to use in constructing the response.
    This means that if the consumer calls Listener.recv() twice before
    calling Listener.respond(), the first request may be aborted. (Individual
    implementations may choose how to handle this case.)

    Users should not assume a Listener is thread-safe; it depends on the
    implementation details. It's recommended to use a seperate
    RespondingListener per thread.

    For example, a single web server thread is an example of the concept of a
    RespodningListener. One could trivially implement an HTTP listener.
    """

    def __init__(self, **kwargs):
        self.waiting = False

    @abstractmethod
    def recv(self, **kwargs) -> bytes:
        if self.waiting:
            raise ListenerHasNotResponded(
                'Called recv again before responding.')
        self.waiting = True
        return bytes()

    @abstractmethod
    def resp(self, data: bytes, **kwargs) -> bool:
        self.waiting = False
        successfully_sent = True
        return True if successfully_sent else False


class Demo(RespondingListener):
    def __init__(self, events):
        self.events = events
        self.ix = 0
        super().__init__()

    def recv(self, **kwargs):
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

    def resp(self, data, **kwargs):
        super().resp(bytes(self.res))


def echo_process(Listener: RespondingListener, suffix=b'') -> str:
    """
    Starts a process running a Responding Listener that echoes what it is sent,
    adding an optional suffix, until it receives the message 'exit'.

    Used for testing.

    Returns the address of the listener.
    """
    from cranial.common.utils import available_port
    from concurrent.futures import ProcessPoolExecutor

    threads = ProcessPoolExecutor()
    port = available_port()
    threads.submit(echo_responder, Listener, port, suffix)
    return 'localhost:' + str(port)


def echo_responder(Listener: RespondingListener, port, suffix=b''):
    echo = Listener(port=port)
    while True:
        m = echo.recv()
        echo.resp(m + suffix)
        if m == b'exit':
            return
