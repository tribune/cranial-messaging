import logging
from time import sleep
from uuid import uuid4

from docopt import docopt

import cranial.common.config as config
from cranial.common.utils import dieIf, warnIf

dieIf("Couldn't load config", config.load, opts=docopt(__doc__),
                                           prefix='cranial_relay',
                                           fname='config.yml')

listener = dieIf("Listener not properly configured",
                 factory, config.get('listener'))

global_storage = dieIf("Storage not properly configured",
                       factory, config.get('storage')) \
                    if config.get('storage') else None 

targets = [dieIf("Target not properly configured", factory, t) 
           for t in config.get('targets')]

sleep_time = config.get('sleep', 1)

while True:
    message = listener.recv()
    if message:
        logging.debug('Received Message: {}', message)
        for t in targets:
            # @TODO use Messenger?
            response = warnIf("Couldn't send", t.notifier.send,
                             **target.config.update(message=message))
            storage = t.storage or global_storage
            if response && storage:
                logging.debug('Storing to {} {}', storage.address, storage.endpoint)
                # @TODO use Messenger?
                warnIf("Couldn't store",
                       storage.send,
                       **storage.config.update(message={'uuid': uuid4(),
                                                        'incoming': message,
                                                        'response': response}))
        sleep_count = 0
    else:
        sleep(sleep_time)
        sleep_count += 1
        if sleep_count % 5 == 0:
            logging.debug("No messages for {} seconds", 
                          sleep_count * sleep_time) 
