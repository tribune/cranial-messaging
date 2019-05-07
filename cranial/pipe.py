#! /usr/bin/python3
"""Usage: pipe.py [--debug] [--echo] [--response] [--ignore-empty] [--config <file>] \
                  [<listener>] [<target>]

Options:
  --debug, -d                 Print config.
  --echo, -e                  Print messages received.
  --response, -r              If the target responds, print it.
  --ignore-empty, -i          Don't send messages consisting of only whitespace.
  --config=<file>, -f=<file>  Config file.

@TODO
Config Example:
listener: module=stdin
target: module=httpget address=localhost:8000 endpoint=hello
"""

import logging
from time import sleep

from docopt import docopt

import cranial.common.config as config
from cranial.common.utils import dieIf, warnIf

opts = docopt(__doc__)

# Conventional syntax
if opts.get('<listener>') == '-':
    opts['<listener>'] = 'stdin://'

if opts.get('<target>') == '-' or opts.get('<target>') is None:
    opts['<target>'] = 'stdout://'

if opts.get('-f'):
    dieIf("Couldn't load config", config.load,
          opts, prefix='cranial_pipe', fname=opts['-f'])
else:
    dieIf("Couldn't load config", config.load,
          opts, prefix='cranial_pipe')


listener = dieIf("Listener not properly configured", config.factory,
                 {**config.get('listener'),
                  **{'package': 'cranial.listeners', 'class': 'Listener'}})

send_params = config.get('target')

target = dieIf("Target not properly configured", config.factory,
               {**send_params,
                **{'package': 'cranial.messaging', 'class': 'Notifier'}})

sleep_time = config.get('sleep', 1)

if opts['--debug']:
    print(config.get())

while True:
    try:
        message = listener.recv()
        if config.get('echo', False):
            print(message)
        if config.get('ignore_empty') and message.strip() == '':
            continue
    except StopIteration:
        break
    if message:
        logging.debug('Received Message: {}', message)
        send_params['message'] = message
        response = warnIf("Couldn't send", target.send, **send_params)
        if response and config.get('response', False):
            print(str(response))
        sleep_count = 0
    else:
        sleep(sleep_time)
        sleep_count += 1
        if sleep_count % 5 == 0:
            logging.debug("No messages for {} seconds",
                          sleep_count * sleep_time)
