from cranial.common import logger

from cranial.listeners import base
from cranial.messaging.adapters.kafka import get_consumer

log = logger.create('kafka_client',
                    logger.fallback('LISTENERS_LOGLEVEL', 'WARNING'))


class Listener(base.Listener):
    """ Listens for messages on a single Kafka topic."""
    def __init__(self, topic, consumer=None, **kwargs):
        self.topic = topic
        if not consumer:
            self.consumer = get_consumer(topic=topic, **kwargs)
        else:
            consumer.subscribe([topic])
            self.consumer = consumer

        self.empty_messages = 0

    def recv(self, timeout=-1, do_raise=False):
        msg = self.consumer.poll(timeout)
        err = hasattr(msg, 'error') and msg.error()
        if err:
            log.warning(err)
            if do_raise:
                raise Exception('Error while polling for message: {}'.format(
                    err))
            # An application might expect streaming bytes. In the future, we
            # might revise this to raise an exception which the application
            # will have to handle itself, as it must already in the None case.
            return b''
        elif hasattr(msg, 'value'):
            return msg.value()
        else:
            alert = 'Consumer poll returned unexpected value: {}'.format(msg)
            if do_raise:
                raise Exception(alert)
            else:
                log.info(alert)
            return None
