"""
@TODO CLI testing w docopt, e.g.: Usage: messenger.py [--local] [TYPE] [PORT]
"""

from abc import ABCMeta, abstractmethod
from random import randint
import requests
import tempfile
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from time import sleep, time
from typing import Any, Dict, List, IO, TYPE_CHECKING  # noqa
import os

from cranial.common import logger
from cranial.servicediscovery import marathon
from cranial.messaging.adapters import firehose

if TYPE_CHECKING:
    import confluent_kafka

log = logger.get('MESSAGING_LOGLEVEL')


class NotifyException(Exception):
    pass


class Notifier(metaclass=ABCMeta):
    # Optionally, @staticmethod
    @abstractmethod
    def send(self, address, message, endpoint):
        return False


class AsyncNotifier(Notifier, metaclass=ABCMeta):
    @abstractmethod
    def finish(self):
        raise Exception('Not Implemented')


class HTTP_Notifier(Notifier):
    @staticmethod
    def send(address, message, endpoint):
        count = 0
        while count < 3:
            response = requests.get('http://{}/{}/{}'.format(
                address, endpoint, message))
            if response.status_code == requests.codes.ok:
                return True
            count += 1
            sleep(5)

        return False


class MockKafkaProducer:
    queue = []  # type: List

    def produce(self, endpoint, message):
        self.queue.append('{}: {}'.format(endpoint, message))

    def flush(self):
        for m in self.queue:
            print(m)
        self.queue = []


reusable_producer_instance = None


def get_reusable_kafka_producer(hosts_csv):
    global reusable_producer_instance
    from cranial.messaging.adapters import kafka as kafka_client
    if not reusable_producer_instance:
        reusable_producer_instance = kafka_client.get_producer(hosts_csv)
    return reusable_producer_instance


class KAFKA_Notifier(Notifier):
    """ This Notifier is a bit odd because the kafka_client is inherently
    asynchronous. The send() method returns once the message is in the client's
    queue. The finish() method returns once the message has been delievered to a
    Kafka broker. So to be truly synchronous, the usage would be:
        KAFKA_Notifier().send(
            address=None, message='Hello', endpoint='channel'
            ).finish()

    In the context of a Messenger, the default behavior is to call finish()
    when it is present, so to be truly asynchronous the usage would be:
        Messenger().notify(message, wait=False)

    ...even when the consumer is using the AsyncKafka notification type.

    I.e., we err on the side of ensuring that the message has been delievered to
    a Broker. If you are willing to risk lost messages in the case of a local
    machine crash or similar failure, then you can use the forms above.

    >>> k = KAFKA_Notifier(client=MockKafkaProducer())
    >>> result = k.send(None, 'foo', 'bar')
    >>> True if result else False
    True
    >>> k.finish()
    bar: foo
    []
    >>> def svc(label):
    ...     return {'any': {'test': ['localhost:0000']}}
    ...
    >>> def proto(label):
    ...     return {'kafka': ['test']}
    ...
    >>> def factory(proto):
    ...    return k
    ...
    >>> m = Messenger(service_discovery=svc,
    ...               protocol_discovery=proto,
    ...               factory=factory)
    ...
    >>> result = m.notify('value')
    key: value
    >>> True if result else False
    True
    >>> m.notify('value2', wait=False)
    True
    """

    def __init__(self,
                 hosts_csv: str = None,
                 client: 'confluent_kafka.Producer' = None) -> None:
        self.client = client or get_reusable_kafka_producer(hosts_csv)

    def send(self, address, message, endpoint):
        """ Note `address` is ignored, since routing is handled by the
        kafka_client.
        """
        log.debug('Sent "{}" to Kafka stream {}.'.format(message, endpoint))
        self.client.produce(endpoint, message)
        return self

    def finish(self):
        self.client.flush()
        return []


class Async_Wrapper(AsyncNotifier):
    def __init__(self, notifier):
        self.threads = []  # type: List[Thread]
        self.results = {}  # type: Dict[str, bytes]
        self.notifier = notifier

    def worker(self, address, message, endpoint):
        self.results[address] = self.notifier.send(address, message, endpoint)

    def send(self, address, message, endpoint):
        t = Thread(target=self.worker, args=(address, message, endpoint))
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

    def send(self, address=None, message=None, endpoint=None):
        f = self.pool.submit(self.notifier.send,
                             address=address,
                             message=message,
                             endpoint=endpoint)
        self.futures.append(f)
        return True

    def finish(self):
        "Returns List of exceptions."
        errors = [f.exception() for f in self.futures]
        self.futures = []
        if hasattr(self.notifier, 'finish'):
            self.notifier.finish()
        return [e for e in errors if e is not None]


class ZMQ_Notifier(Notifier):
    def send(self, address, message, endpoint):
        from cranial.messaging.adapters.zmq import send_string
        return send_string(message, address, wait=False)


class LocalDisk_Notifier(Notifier):
    """ Write messages to a local file named `address`

    Tested in LocalMessenger().
    """
    logfiles = {}  # type: Dict[str, IO]

    def send(self, address, message, endpoint=None):
        try:
            if address not in self.logfiles.keys() \
                    or self.logfiles[address].closed:
                # make sure the path exists
                d, _ = os.path.split(address)
                if d != '':
                    os.makedirs(d, exist_ok=True)
                self.logfiles[address] = open(address, 'a')

            return self.logfiles[address].write(message + '\n')
        except Exception as e:
            raise NotifyException("{} || address: {} || message: {}".format(
                e, address, message))

    def finish(self):
        for _, fh in self.logfiles.items():
            fh.flush()

    def __del__(self):
        for _, fh in self.logfiles.items():
            fh.close()


class FIREHOSE_Notifier(Notifier):
    def __init__(self):
        self.client = firehose.get_client()

    def send(self, address, message, endpoint):
        self.firehose.put_record(DeliveryStreamName=endpoint,
                                 Record={'Data': bytes(message, 'utf8')})
        return True


def default_factory(proto: str):
    # Avoid case-sensitivity.
    proto = proto.upper()

    # Kafka is inherently Async; wrapping is unnecessary overheard.
    assert not ('ASYNC' in proto and 'KAFKA' in proto)

    # Find the Class that handles this Notifiction type.
    class_name = proto.replace('ASYNC', '') + '_Notifier'
    g = globals()
    notifier = g[class_name]() if class_name in g else KAFKA_Notifier()

    if 'ASYNC' in proto:
        notifier = Async_Wrapper(notifier)

    return notifier


class Messenger():
    """ @TODO Switch to cranial.servicediscovery"""

    def __init__(self, endpoint='key', label='CONTENT_PROCESSOR',
                 opts={'DEPRECATED': 1},
                 # service_discovery: Discovery = marathon.Discovery,
                 service_discovery=marathon.get_tasks_by_label,
                 protocol_discovery=None,
                 factory=None):
        """

        Parameters
        ----------

        endpoint:
            string. Passed to Notifier.send().

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
        self.services = service_discovery
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
            mtype = self.opts.get('TYPE', 'http')
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
            if type(notifier) is KAFKA_Notifier:
                log.error('A service requested messages to all nodes via Kafka.'
                          + ' Kafka consumers should use "any" mode.')
                break  # @TODO log or something.
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
            if type(notifier) is KAFKA_Notifier:
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
                    fails = notifier.finish()
                    if len(fails) > 0:
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

    def __init__(self, endpoint='log', async=False,
                 *args, **kwargs):
        self.endpoint = endpoint
        self.label = 'NA'
        self.hosts = {'time': 0}
        self.opts = {}
        tmpdir = dir or tempfile.mkdtemp()
        self.services = lambda _: {'all': {'local': [tmpdir]}}
        self.protocols = lambda _: {'AsyncLocalDisk' if async else 'LocalDisk':
                                    self.services('NA')['all']}
        self.factory = lambda _: LocalDisk_Notifier()
