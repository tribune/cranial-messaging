"""
@TODO CLI testing w docopt, e.g.: Usage: messenger.py [--local] [TYPE] [PORT]
"""

from abc import ABCMeta, abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor
import importlib
from random import randint
from threading import Thread
from time import time
from typing import Any, Dict, List, IO, Optional, Set, Tuple, TYPE_CHECKING  # noqa

from cranial.common import logger
import cranial.servicediscovery.base as sd
from cranial.servicediscovery import marathon


log = logger.get('MESSAGING_LOGLEVEL')


class NotifyException(Exception):
    pass


class Notifier(metaclass=ABCMeta):
    def __init__(self, **kwargs):
        pass

    # Optionally, @staticmethod
    @abstractmethod
    def send(self,
             address: Optional[str],
             message: str,
             endpoint: Optional[str],
             **kwargs):
        return False


class AsyncNotifier(Notifier, metaclass=ABCMeta):
    @abstractmethod
    def finish(self):
        raise Exception('Not Implemented')


class Async_Wrapper(AsyncNotifier):
    def __init__(self, notifier):
        self.threads = []  # type: List[Thread]
        self.results = {}  # type: Dict[str, bytes]
        self.notifier = notifier

    def worker(self, address, message, endpoint, kwargs):
        self.results[address] = self.notifier.send(
                address, message, endpoint, **kwargs)

    def send(self, address=None, message=None, endpoint=None, **kwargs):
        t = Thread(
              target=self.worker, args=(address, message, endpoint, kwargs))
        self.threads.append(t)
        t.start()
        return True

    def finish(self):
        "Returns List of failed addresses."
        for t in self.threads:
            t.join()
        if hasattr(self.notifier, 'finish'):
            self.notifier.finish()

        return [x[0] for x in self.results.items() if x[1] is False]


class Async_WrapperPool(AsyncNotifier):
    def __init__(self, notifier, n_threads=1):
        self.n_threads = n_threads
        self.futures = []  # type: List
        self.notifier = notifier
        self.pool = ThreadPoolExecutor(self.n_threads)

    def send(self, address=None, message=None, endpoint=None, **kwargs):
        f = self.pool.submit(self.notifier.send,
                             address=address,
                             message=message,
                             endpoint=endpoint,
                             **kwargs)
        self.futures.append(f)
        return True

    def finish(self):
        "Returns List of exceptions."
        errors = [f.exception() for f in self.futures]
        self.futures = []
        if hasattr(self.notifier, 'finish'):
            self.notifier.finish()
        return [e for e in errors if e is not None]


def default_factory(proto: str) -> Notifier:
    # Avoid case-sensitivity.
    proto = proto.lower()

    # Kafka is inherently Async; wrapping is unnecessary overheard.
    assert not ('async' in proto and 'kafka' in proto)

    # Find the module that handles this Notifiction type.
    name = proto.replace('async', '')
    mod = importlib.import_module('cranial.messaging.' + name)
    notifier = mod.Notifier()  # type: ignore

    if 'async' in proto:
        notifier = Async_Wrapper(notifier)

    return notifier


ServiceName = str


class Messenger():
    time = 0
    sent_success = set()  # type: Set[Tuple[str, str, str]]

    def __init__(
          self,
          endpoint: str = 'key',
          discovery: sd.Discovery = None,
          factory=None, **kwargs) -> None:
        """
        Parameters
        ----------

        endpoint:
            Passed to Notifier.send(). @TODO Registered services should
            be able to override?

        discovery:
            cranial.servicediscovery.base.Discovery

        factory:
            A function that takes a protocol string and returns a Notifier().
        """
        self.endpoint = endpoint
        self.discovery = discovery or marathon.Discovery('CONTENT_PROCESSOR')
        self.update_consumers()
        self.factory = factory if factory else default_factory

    def get_notifier_for_service(self, svc) -> Notifier:
        # @TODO Create a Notifier Pool so we can reuse them?
        proto = self.discovery.get_protocol(svc)
        return self.factory(proto)

    def update_consumers(self):
        self.discovery.update()
        self.time = time()

    def notify(self, message, wait=True):
        # Update hosts every 5 minutes, regardless.
        if time() - self.time > 300:
            self.update_consumers()

        threads = {}  # type: Dict[ServiceName, Notifier]
        failed = []  # type: List[ServiceName]
        for svc in self.discovery.services:
            if self.discovery.get_mode(svc) == 'all':
                t, f = self.notify_all(svc, message)
            else:
                t, f = self.notify_any(svc, message)
            threads.update(t)
            failed.extend(f)

        # Reset cache of delieveries.
        self.sent_success = set()

        # Mode: close? only-local?
        # @TODO Notify local instance, if possible.
        # Marathon's Spartan L4LB might handle this automatically if we used
        # hostnames instead of IPs.

        # Get results of any Async notifications.
        if wait:
            for svc, notifier in threads.items():
                if hasattr(notifier, 'finish'):
                    fails = notifier.finish()  # type: ignore
                    if fails and len(fails) > 0:
                        failed.append('{svc}({hosts})'.format(
                            svc=svc, hosts=','.join(fails)))

        if len(failed) > 0:
            log.warn('Failed Notifications: {}'.format(failed))
            raise NotifyException('Failed to notify services/hosts: ' +
                                  ' '.join(failed))
        return wait or threads

    def notify_all(self, svc, message: str) -> Tuple[Dict, List]:
        threads = {}  # type: Dict[ServiceName, Notifier]
        failed = []  # type: List[ServiceName]
        for inst in self.discovery.get_instances(svc):
            notifier = self.get_notifier_for_service(svc)
            threads[svc] = notifier
            log.debug(
                'Attempt notify to "all" {}, instance {} via {}.'.format(
                    svc, inst, type(notifier)))
            success = notifier.send(inst, message, self.endpoint)
            if not success:
                self.update_consumers()
                failed.append(svc)
        return threads, failed

    def notify_any(self, svc, message: str) -> Tuple[Dict, List]:
        threads = {}  # type: Dict[ServiceName, Notifier]
        failed = []  # type: List[ServiceName]

        instances = self.discovery.get_instances(svc)
        if len(instances) == 0:
            return threads, failed
        # @TODO Accept optional Instance Selection function.
        i = randint(0, len(instances) - 1)
        notifier = self.get_notifier_for_service(svc)
        threads[svc] = notifier
        log.debug(
          'Attempt notify to "any" {}, chosen instance {} via {}.'.format(
                svc, instances[i], type(notifier)))

        # @TODO Most notifiers should pick local if possible?
        target = instances[i]

        # Sometimes multiple subscribed services will use the same address
        # and endpoint, e.g., a Kafka broker. In this case, we don't want
        # to send the mesage more than once.
        # To best support this, all services that rely on distributed brokers
        # should register with the same host or set of hosts.
        delivery = (','.join(sorted(instances)), message, self.endpoint)
        if delivery in self.sent_success:
            return {}, []
        else:
            success = notifier.send(target, message, self.endpoint)

        if not success:
            self.update_consumers()
            target = instances[i - 1] if i > 0 else instances[-1]
            success = notifier.send(target, message, self.endpoint)
        if not success:
            failed.append(svc)
        else:
            self.sent_success.add(delivery)

        return threads, failed


class LocalMessenger(Messenger):
    """Writes to Local disk, instead of using service discovery.
    The filename will be set by the `endpoint` parameter.

    This class exists for:
        1. Debugging usage
        2. An example of sub-classing Messenger
        3. Testing Messenger and file.Notifier via the following doctest.

    >>> import tempfile
    >>> d = tempfile.mkdtemp()
    >>> m = LocalMessenger(endpoint=d+'/log', mode='any')
    >>> m.notify('hello')
    True
    >>> m.notify('world')
    True
    >>> with open(d+'/log') as fh:
    ...     fh.readlines()
    ...
    ['hello\\n', 'world\\n']
    >>> d = tempfile.mkdtemp()
    >>> # A hack to test 'all' mode by writing to the same log file twice.
    >>> m = LocalMessenger(endpoint=d+'/log', mode='all', hosts=['/','/'])
    >>> m.notify('hello')
    True
    >>> with open(d+'/log') as fh:
    ...     fh.readlines()
    ...
    ['hello\\n', 'hello\\n']
    """

    def __init__(self, endpoint='log', wait=True, mode='all', hosts=None,
                 *args, **kwargs):
        hosts = hosts or ['localhost']
        self.endpoint = endpoint
        self.discovery = sd.PythonDiscovery({'local': {
            'hosts': hosts,
            'protocol': 'File' if wait else 'AsyncFile',
            'mode': mode}})
        self.factory = default_factory


class MessengerExecutor(Executor):
    def __init__(self,
                 label='worker_pool',
                 discovery: sd.Discovery = None) -> None:
        pass

    def submit(self, fn, *args, **kwargs):
        # WIP
        pass

    def _my_map(self, func, *iterables, timeout=None, chunksize=1):
        for i in range(0, len(iterables[0])):
            args = []
            for a in iterables:
                try:
                    args.append(a[i])
                except KeyError:
                    break
            self.submit(func, *args)
