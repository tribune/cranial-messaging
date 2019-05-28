#! /usr/bin/python3
"""Usage: pipe.py [--debug] [--echo] [--response] [--ignore-empty] \
                  [--config <file>] [--list] [<listener>] [<target>]

Options:
  --echo, -e                  Print messages received.
  --response, -r              If the target responds, print it.
  --ignore-empty, -i          Don't send messages of only whitespace.
  --config=<file>, -f=<file>  Config file.
  --list, -l                  List supported protocols & exit.

@TODO
Config Example:
listener: module=stdin
target: module=httpget address=localhost:8000 endpoint=hello
sleep: 10
"""

import json
from time import sleep

from docopt import docopt

import cranial.common.config as config
import cranial.common.logger as logger
from cranial.common.utils import dieIf, warnIf

logging = logger.get()

opts = docopt(__doc__)

if opts.get('--list'):
    import pkgutil
    import cranial.listeners as l
    import cranial.messaging as m
    print('Built-in Protocols\n==================')
    for pkg, name in [(l, "Listeners"), (m, "Notifiers")]:
        print("\n" + name + "\n----------------")
        prefix = pkg.__name__ + '.'
        for info in pkgutil.iter_modules(pkg.__path__, prefix):
            mod = info.name.split('.')[-1]
            if mod not in ['base']:
                print(mod)
    exit()


# Conventional syntax
if opts.get('<listener>') == '-':
    opts['<listener>'] = 'stdin://'
elif opts.get('<listener>') is None:
    print("At least a listener is required. Use for --help or --list to see " +
          "supported listeners & notifiers.")
    exit(1)

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
        if type(message) not in [str, bytes]:
            msgstr = json.dumps(message)
        else:
            msgstr = str(message)
        if config.get('echo', False):
            print(message)
        if config.get('ignore_empty', True) and msgstr.strip() == '':
            continue
    except StopIteration:
        break
    if message:
        logging.debug('Received Message: %s', message)
        send_params['message'] = message
        # response = warnIf("Couldn't send", target.send, **send_params)
        response = target.send(**send_params)
        if response and config.get('response', False):
            print(str(response))
        sleep_count = 0
    else:
        sleep(sleep_time)
        sleep_count += 1
        if sleep_count % 5 == 0:
            logging.debug("No messages for %s seconds",
                          sleep_count * sleep_time)
