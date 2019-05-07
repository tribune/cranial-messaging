from typing import List, TYPE_CHECKING  # noqa

from cranial.common import logger
from cranial.messaging import base

if TYPE_CHECKING:
    import confluent_kafka

log = logger.get('MESSAGING_LOGLEVEL')

reusable_producer_instance = None


class MockKafkaProducer:
    queue = []  # type: List

    def produce(self, endpoint, message):
        self.queue.append('{}: {}'.format(endpoint, message))

    def flush(self):
        for m in self.queue:
            print(m)
        self.queue = []


def get_reusable_kafka_producer(hosts_csv):
    global reusable_producer_instance
    from cranial.messaging.adapters import kafka as kafka_client
    if not reusable_producer_instance:
        reusable_producer_instance = kafka_client.get_producer(hosts_csv)
    return reusable_producer_instance


class Notifier(base.Notifier):
    """ This Notifier is a bit odd because the kafka_client is inherently
    asynchronous. The send() method returns once the message is in the client's
    queue. The finish() method returns once the message has been delievered to a
    Kafka broker. So to be truly synchronous, the usage would be:
        Notifier().send(
            address=None, message='Hello', endpoint='channel'
            ).finish()

    In the context of a Messenger, the default behavior is to call finish()
    when it is present, so to be truly asynchronous the usage would be:
        Messenger().notify(message, wait=False)

    ...even when the consumer is using the AsyncKafka notification type.

    I.e., we err on the side of ensuring that the message has been delievered to
    a Broker. If you are willing to risk lost messages in the case of a local
    machine crash or similar failure, then you can use the forms above.

    >>> k = Notifier(client=MockKafkaProducer())
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
    >>> m = base.Messenger(service_discovery=svc,
    ...                    protocol_discovery=proto,
    ...                    factory=factory)
    ...
    >>> result = m.notify('value')
    key: value
    >>> True if result else False
    True
    >>> threads = m.notify('value2', wait=False)
    >>> 'test' in threads and type(threads['test'])
    <class 'kafka.Notifier'>
    """

    def __init__(self,
                 hosts_csv: str = None,
                 client: 'confluent_kafka.Producer' = None) -> None:
        self.client = client or get_reusable_kafka_producer(hosts_csv)

    def send(self, address, message, endpoint, **kwargs):
        """ Note `address` is ignored, since routing is handled by the
        kafka_client.
        """
        log.debug('Sent "{}" to Kafka stream {}.'.format(message, endpoint))
        self.client.produce(endpoint, message)
        return self

    def finish(self):
        self.client.flush()
        return []
