import datetime
import os
import socket

import confluent_kafka as kafka

from cranial.common import logger

log = logger.create('cranial', os.environ.get('CRANIAL_LOGLEVEL', 'WARN'))


class LoggingProducer(kafka.Producer):
    def logError(self, err, msg):
        if err:
            log.error(msg)

    def produce(self, *args, **kwargs):
        kwargs['on_delivery'] = self.logError
        super().produce(*args, **kwargs)


class MustCommitConsumer(kafka.Consumer):
    def __init__(self, config):
        config['enable.auto.commit'] = False
        super().__init__(config)


class CarefulConsumer(MustCommitConsumer):
    """A Kafka consumer that refrains from commiting it's previous message until
    it is asking for a new one. Threads should not share a CarefulConsumer!

    >>> from confluent_kafka import TopicPartition
    >>> from uuid import uuid4
    >>> cluster = run_local_docker_kafka()
    >>> t = str(uuid4())
    >>> h = 'localhost:9092'
    >>> c1 = get_consumer(topic=t, hosts=h, group=t, reset='earliest')
    >>> p = get_producer(hosts_csv=h)
    >>> p.produce(t, value='a')
    >>> p.produce(t, value='b')
    >>> p.produce(t, value='c')
    >>> p.flush()
    0
    >>> del p
    >>> first = c1.poll(60)
    >>> while first.value() == b'': first = c1.poll(1) # Sometimes 1st is empty.
    >>> first.value()
    b'a'
    >>> tps = [TopicPartition(first.topic(), first.partition())]
    >>> c1.position(tps)[0].offset
    1
    >>> m1 = c1.poll(1)
    >>> c1.position(tps)[0].offset
    2
    >>> m1.value()
    b'b'
    >>> c1.close() # We close without committing message 'm1'.
    >>> c2 = get_consumer(t, hosts=h, group=t, reset='earliest')
    >>> m2 = c2.poll(60)
    >>> while m2.value() == b'': m2 = c2.poll(1) # Sometimes 1st is empty.
    >>> m2.value() == m1.value()
    True
    >>> c2.commit(m2, async=False)
    >>> c2.last_message == None # Because we just committed it.
    True
    >>> c2.close()
    >>> c3 = get_consumer(t, hosts=h, group=t, reset='earliest')
    >>> m3 = c3.poll(60)
    >>> while m3.value() == b'': m3 = c3.poll(1) # Sometimes 1st is empty.
    >>> m3.value()
    b'c'
    >>> c3.close()
    >>> shutdown_local_docker_kafka(cluster)
    """

    def __init__(self, config):
        super().__init__(config)
        self.last_message = None

    def __del__(self):
        """Ensure last message is committed on garbage collection."""
        self.close()
        if hasattr(super(), '__del__'):
            super().__del__()

    def close(self):
        """Ensure last message is committed on close."""
        if self.last_message:
            self.commit(self.last_message)
        try:
            super().close()
        except RuntimeError:
            pass

    def poll(self, timeout=-1):
        if self.last_message:
            self.commit(self.last_message)
        self.last_message = super().poll(timeout)
        return self.last_message

    def commit(self, *args, **kwargs):
        super().commit(*args, **kwargs)
        if args[0] == self.last_message or \
                kwargs['message'] == self.last_message:
            self.last_message = None

    def assign(self, *args, **kwargs):
        """ Overridden because this is unneeded and difficult to use correctly.
        """
        raise Exception("Not supported.")

    def unassign(self, *args, **kwargs):
        """ Overridden because this is unneeded and difficult to use correctly.
        """
        raise Exception("Not supported.")


def get_bootstrappers():
    """ @TODO Move this into service discovery?"""
    env = os.environ.get('CRANIAL_KAFKA_BOOTSTRAPPERS')
    if env:
        return env
    else:
        log.warning('No Kafka Hosts found. Set CRANIAL_KAFKA_BOOTSTRAPPERS.')


def get_producer(hosts_csv: str = None):
    bootstrappers = hosts_csv or get_bootstrappers()
    return LoggingProducer({
        'bootstrap.servers': bootstrappers,
        # librdkafka is noisey about disconnects that are OK.
        'log.connection.close': False})


def get_solo_consumer(topic: str, hosts: str = None):
    """ Most common usages: A single consumer, not part of a group,
    that starts reading new messages only, on a single topic.

    hosts: A CSV list of broker hosts.

    """
    log.warning('DEPRECATED: get_solo_consumer(). Use get_consumer().')
    return get_consumer(topic, hosts)


def get_group_consumer(topic: str, group, hosts: str = None):
    log.warning('DEPRECATED: get_group_consumer(). ' +
                'Use get_consumer(group=str).')
    return get_consumer(topic, group, hosts)


def get_consumer(topic: str,
                 group: str = None,
                 hosts: str = None,
                 reset: str = 'latest',
                 config={},
                 constype: type = CarefulConsumer):
    """ A consumer, optionally in a group, on a single topic."""
    group = group or '{date:%Y-%m-%d_%H:%M:%S}_{host}'.format(
                       date=datetime.datetime.now(),
                       host=socket.gethostname())

    bootstrappers = hosts or get_bootstrappers()

    timeout = int(os.environ.get('CRANIAL_KAFKA_TIMEOUT_MS', 30 * 1000))
    # Per Kafka docs, heartbeat interval should be No More Than 1/3 session
    # timeout. We set a maximum of 5 seconds.
    heartbeat = int(timeout / 3)
    heartbeat = heartbeat if heartbeat < 5000 else 5000

    combined_config = {
        'bootstrap.servers': bootstrappers,
        'group.id': group,
        # librdkafka is noisey about disconnects that are OK.
        'log.connection.close': False,
        'session.timeout.ms': timeout,
        'heartbeat.interval.ms': heartbeat,
        'default.topic.config': {'auto.offset.reset': reset}}
    combined_config.update(config)
    c = constype(combined_config)
    c.subscribe([topic])
    return c


def run_local_docker_kafka():
    from subprocess import run
    from time import sleep

    zk = run(['docker', 'run', '-d', '--name', 'zookeeper',
              '-p', '2181:2181', 'confluent/zookeeper'])
    sleep(3)  # Time for the server to wake up.
    kf = run(['docker', 'run', '-d', '--name', 'kafka',
              '-p', '9092:9092', '--link', 'zookeeper',
              '--hostname', 'localhost', 'confluent/kafka'])
    sleep(3)  # Time for the server to wake up.
    return (zk, kf)


def shutdown_local_docker_kafka(cluster):
    from subprocess import run
    zk, kf = cluster
    if kf.returncode == 0:
        run(['docker', 'rm', '-f', 'kafka'])
    if zk.returncode == 0:
        run(['docker', 'rm', '-f', 'zookeeper'])
