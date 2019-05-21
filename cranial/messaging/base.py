"""
@TODO CLI testing w docopt, e.g.: Usage: messenger.py [--local] [TYPE] [PORT]
"""

from abc import ABCMeta, abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor
import importlib
from random import randint
import tempfile
from threading import Thread
from time import time
from typing import Any, Dict, List, IO, Optional, TYPE_CHECKING  # noqa

from cranial.common import logger
import cranial.servicediscovery.base
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


class Messenger():
    """ @TODO Switch to cranial.servicediscovery"""

    def __init__(self, endpoint='key', label='CONTENT_PROCESSOR',
                 opts={'DEPRECATED': 1},
                 # service_discovery: Discovery = marathon.Discovery,
                 service_discovery=None,
                 protocol_discovery=None,
                 factory=None):
        """

        Parameters
        ----------

        endpoint:
            string. Passed to Notifier.send(). @TODO Registered services should
            be able to override.

        label:
            string. Passed to service_discovery() and protocol_discovery().

        opts:
            dict. As produced by docopt. Uses '--local' to override
            service discovery. @DEPRECATED

        service_discovery:
            A function that accepts 'label' and returns a dictionary of the
            form:
                {'any': {'$service_id': ['$host_ip:$port', ...], ...},
                 'all': {'$service_id': ['$host_ip:$port', ...], ...}}

        protocol_discovery:
            Deprecated. Will be part of new ServiceDiscovery object metadata.
            A function that accepts 'label' and returns a dictionary of the
            form:
                {'http': {'$service_id': ['$host_ip:$port', ...], ...},
                 'kafka': {'$service_id': ['$host_ip:$port', ...], ...},
                 ...}

        factory:
            A function that takes a protocol string and returns a Notifier().

        """
        self.endpoint = endpoint
        self.label = label
        self.hosts = {'time': 0}  # type: Dict[str, Any]
        self.opts = opts
        self.services = service_discovery or marathon.get_tasks_by_label
        self.protocols = protocol_discovery if protocol_discovery \
            else lambda _: marathon.get_tasks_by_label('NOTIFIER')
        self.factory = factory if factory else default_factory

    def get_notifier_for_service(self, svc) -> Notifier:
        registry = self.hosts['data']['protocols']
        proto = None
        for p in registry:
            if svc in registry[p]:
                proto = p
                break
        proto = proto or 'kafka'

        return self.factory(proto)

    def update_consumers(self):
        # Don't spam the service discovery.
        if time() - self.hosts['time'] < .5:
            return
        if self.opts.get('--local'):
            port = self.opts.get('PORT', '5000')
            mtype = self.opts.get('TYPE', 'httpget')
            self.hosts = {'time': time(),
                          'data': {'all': {'local': ['localhost:' + port]},
                                   'any': {},
                                   'protocols': {mtype: 'local'}
                                   },
                          }
        else:
            self.hosts['time'] = time()
            self.hosts['data'] = self.services(self.label)
            self.hosts['data']['protocols'] = self.protocols(self.label)

        log.debug(self.hosts)

    def new_notify(self, message, wait=True):
        raise Exception('WIP. Nothing should be calling this yet.')
        # Scaffolding for new ServiceDiscovery-based notifications.
        results = {}
        for svc in self.discovery.services:
            if self.discovery.get_metadata(svc, self.label) == 'all':
                results[svc] = self.notify_all(svc)
            else:
                results[svc] = self.notify_any(svc)

        return results

    def notify(self, message: str, wait=True):
        # Update hosts every 5 minutes, regardless.
        if time() - self.hosts['time'] > 300:
            self.update_consumers()

        failed = set()
        threads = {}
        # Mode: all.
        for svc, instances in self.hosts['data'].get('all', {}).items():
            if len(instances) == 0:
                continue
            notifier = self.get_notifier_for_service(svc)
            threads[svc] = notifier
            for inst in instances:
                log.debug(
                    'Attempt notify to "all" {}, instance {} via {}.'.format(
                        svc, inst, type(notifier)))
                success = notifier.send(inst, message, self.endpoint)
                if not success:
                    self.update_consumers()
                    failed.add(svc)

        # Mode: any.
        sent_kafka = False
        for svc, instances in self.hosts['data'].get('any', {}).items():
            if len(instances) == 0:
                continue
            i = randint(0, len(instances) - 1)
            notifier = self.get_notifier_for_service(svc)
            threads[svc] = notifier
            log.debug(
              'Attempt notify to "any" {}, chosen instance {} via {}.'.format(
                    svc, instances[i], type(notifier)))

            # I don't like this ugly hack for Kafka. :-(
            # We should use service meta-data for a service to declare
            # what modes it supports. ServiceDiscovery should solve this. @TODO
            if notifier.__module__ == 'kafka':
                if sent_kafka:
                    continue
                else:
                    success = notifier.send(None, message, self.endpoint)
                    sent_kafka = success
            else:
                # @TODO Most notifiers should pick local if possible?
                success = notifier.send(instances[i], message, self.endpoint)

            if not success:
                self.update_consumers()
                alt = instances[i - 1] if i > 0 else instances[-1]
                success = notifier.send(alt, message, self.endpoint)
            if not success:
                failed.add(svc)

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
                        failed.add('{svc}({hosts})'.format(
                            svc=svc, hosts=','.join(fails)))

        if len(failed) > 0:
            log.warn('Failed Notifications: {}'.format(failed))
            raise NotifyException('Failed to notify services/hosts: ' +
                                  ' '.join(failed))
        return wait or threads


class LocalMessenger(Messenger):
    """Writes to Local disk, instead of using service discovery.
    The filename will be set by the `endpoint` parameter.

    This class exists for:
        1. Debugging usage
        2. An example of sub-classing Messenger
        3. Testing Messenger via the following doctest.

    >>> import tempfile
    >>> d = tempfile.mkdtemp()
    >>> m = LocalMessenger(endpoint=d+'/log')
    >>> m.notify('hello')
    True
    >>> m.notify('world')
    True
    >>> with open(d+'/log') as fh:
    ...     fh.readlines()
    ...
    ['hello\\n', 'world\\n']
    """

    def __init__(self, endpoint='log', wait=True,
                 *args, **kwargs):
        self.endpoint = endpoint
        self.label = 'NA'
        self.hosts = {'time': 0}
        self.opts = {}
        tmpdir = tempfile.mkdtemp()
        self.services = lambda _: {'all': {'local': [tmpdir]}}
        self.protocols = lambda _: {'LocalDisk' if wait else 'AsyncLocalDisk':
                                    self.services('NA')['all']}
        self.factory = lambda _: default_factory('localdisk')


class MessengerExecutor(Executor):
    def __init__(self,
                 label='worker_pool',
                 discovery: cranial.servicediscovery.base.Discovery = None) -> None:
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
