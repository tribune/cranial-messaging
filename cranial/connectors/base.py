from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
import io
import os
from tempfile import mkstemp
from typing import Any, Callable, Dict, Iterator, List, Set, Union  # noqa

SomeIO = Union[io.StringIO, io.BytesIO]


class Connector():
    def __init__(self, base_address='', binary=True, do_read=False):
        self.base_address = base_address
        self.binary = binary
        self.do_read = do_read  # @TODO Deprecated!

    def iterator(self, target: Any) -> Iterator:
        """Get one or more IOstreams and iterate over them yielding serialized
        records.
        """
        if type(target) == list:
            return self.generate_from_list(target)
        else:
            f = self.getFuture(target)
            return self.generate(f)

    def generate(self, future_item: Future):
        for l in future_item.result():
            yield l

    def generate_from_list(self, l: List) -> Iterator:
        # getMultiple on all items, start returning whatever item arrives 1st.
        future_items = self.getMultiple(dict(enumerate(l)), blocking=False)
        done = set()  # type: Set[int]
        while len(done) < len(future_items):
            for i in future_items:
                if not (i in done):
                    try:
                        data = future_items[i].result(1)
                        for record in data:
                            yield record
                        done.add(i)
                    except futures.TimeoutError:
                        pass
                    except futures.CancelledError:
                        done.add(i)
        return

    def toStream(self, data) -> SomeIO:
        if isinstance(data, str):
            return io.StringIO(data)
        elif isinstance(data, bytes):
            return io.BytesIO(data)
        else:
            raise Exception('Not implemented for data type: {}'.format(
                type(data)))

    def get_tmp_file(self):
        fd, local_path = mkstemp()
        os.close(fd)
        return fd, local_path

    def get(self, name=None) -> io.BytesIO:
        """Return an IOstream."""
        raise Exception('Not Implemented')

    def put(self, stream: io.BytesIO, name: str = None) -> bool:
        """Return True if successful."""
        raise Exception('Not Implemented')

    def executor(self):
        if not hasattr(self, 'pool'):
            self.pool = ThreadPoolExecutor()
        return self.pool

    def _doFuture(self, fn: Callable, *args, **kwargs) -> Future:
        return self.executor().submit(fn, *args, **kwargs)

    def getFuture(self, *args, **kwargs) -> Future:
        return self._doFuture(self.get, *args, **kwargs)

    def putFuture(self, *args, **kwargs) -> Future:
        return self._doFuture(self.put, *args, **kwargs)

    def _doMultiple(
            self,
            fn: Callable,
            d: Dict,
            blocking=True,
            **kwargs) -> Union[Dict[Any, Future], Dict[Any, io.IOBase]]:
        """Takes a dict of keys to argument to pass to fn.

        Returns dict of Futures if 'blocking' is set False. Otherwise,
        IOStreams.
        """
        response = {}
        for key in d:
            args = d[key]
            # Gracefully accept single arguments as-is.
            if type(args) is not list:
                args = [args]
            response[key] = fn(*args, **kwargs)

        if not blocking:
            return response

        done = set()  # type: Set[Any]
        while len(done) < len(d):
            for key in d:
                if not (key in done):
                    try:
                        data = response[key].result(1)
                        response[key] = data
                        done.add(key)
                    except futures.TimeoutError:
                        pass
                    except futures.CancelledError:
                        response[key] = False
                        done.add(key)
        return response

    def getMultiple(self, d: dict, blocking=True, **kwargs):
        return self._doMultiple(self.getFuture, d, blocking, **kwargs)

    def putMultiple(self, d: dict, blocking=True, **kwargs):
        return self._doMultiple(self.putFuture, d, blocking, **kwargs)

    def __str__(self):
        ss = [str(self.__class__).split("'")[1]]
        for attr in dir(self):
            if (not attr.startswith("_")) and (
                    not hasattr(getattr(self, attr), '__call__')):
                ss.append("{} = {}".format(attr, getattr(self, attr))[:100])
        return '\t'.join(ss)
