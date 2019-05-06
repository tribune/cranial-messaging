"""Usage: pipe.py -f=<file> [<listener>] [<target>]

Options:
  -f=<file>  Config file.

Config Example:
listener: module=stdin
target: module=httpget address=localhost:8000 endpoint=hello
"""

import logging
from time import sleep

from docopt import docopt

import cranial.common.config as config
from cranial.common.utils import dieIf, warnIf

dieIf("Couldn't load config", config.load,
      opts=docopt(__doc__), prefix='cranial_pipe', fname='config.yml')

listener = dieIf("Listener not properly configured",
                 config.factory,
                 config.get('listener').update({'class': 'Listener'}))

target = dieIf("Target not properly configured", config.factory,
               config.get('target').update({'class': 'Notifier'}))

sleep_time = config.get('sleep', 1)

while True:
    message = listener.recv()
    if message:
        logging.debug('Received Message: {}', message)
        response = warnIf("Couldn't send", target.notifier.send,
                          **target.config.update(message=message))
        sleep_count = 0
    else:
        sleep(sleep_time)
        sleep_count += 1
        if sleep_count % 5 == 0:
            logging.debug("No messages for {} seconds",
                          sleep_count * sleep_time)
