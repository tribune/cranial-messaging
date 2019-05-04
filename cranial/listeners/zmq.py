# 3rd-party modules.
import zmq

# 1st party
from cranial.common import logger
from cranial.listeners import base

log = logger.create('listeners',
                    logger.fallback('LISTENERS_LOGLEVEL', 'WARNING'))


class Listener(base.RespondingListener):
    def __init__(self, port=5678, **kwargs):
        self.context = zmq.Context.instance()
        self.server = self.context.socket(zmq.ROUTER)
        self.server.bind("tcp://*:" + str(port))
        log.info('Running ZMQ Server on port ' + str(port))
        super().__init__()

    def recv(self, wait=True, **kwargs):
        if wait:
            super().recv()
        address, _, msg = self.server.recv_multipart()
        self.address = address
        return msg

    def resp(self, data: bytes, **kwargs):
        self.server.send_multipart([
            self.address,
            b'',
            data
        ])
        super().resp(None)
        return True
