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
from cranial.common.utils import dieIf

logging = logger.get()

opts = docopt(__doc__)

if opts.get('--list'):
    import pkgutil
    import cranial.listeners as L
    import cranial.messaging as M
    print('Built-in Protocols\n==================')
    for pkg, name in [(L, "Listeners"), (M, "Notifiers")]:
        print("\n" + name + "\n----------------")
        prefix = pkg.__name__ + '.'
        for info in pkgutil.iter_modules(pkg.__path__, prefix):
            mod = info.name.split('.')[-1]
            if mod not in ['base', 'file']:
                print(mod)
        # Protocols via smart_open in the File modules:
        for i in ('file', 's3', 'hdfs', 'webhdfs', 'ssh | scp | sftp'):
            print(i+'*' if pkg == L else i)

    print("\n* These protocols support auto decompression from gzip and " +
          "bzip2 formats.")
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


if opts['--debug']:
    print(config.get())

try:
    listener = config.factory(
        {**config.get('listener'),
         **{'package': 'cranial.listeners', 'class': 'Listener'}})
except TypeError as e:
    listener = config.get('listener')
    if type(listener) is str:
        # Maybe it's a filename?
        listener = dieIf("Listener not properly configured",
                         config.factory,
                         {'package': 'cranial.listeners',
                          'module': 'file',
                          'class': 'Listener',
                          'path': listener})
    else:
        raise(e)
except ModuleNotFoundError:
    listener_str = config.get('listener_str')
    logging.info('Trying smart_open for URI: %s', listener_str)
    listener = dieIf("Listener not properly configured",
                     config.factory,
                     {**config.get('listener'),
                      **{'package': 'cranial.listeners',
                         'class': 'Listener',
                         'module': 'file',
                         'path': listener_str}})


send_params = config.get('target')

target = dieIf("Target not properly configured", config.factory,
               {**send_params,
                **{'package': 'cranial.messaging', 'class': 'Notifier'}})

sleep_time = config.get('sleep', 1)


while True:
    try:
        message = listener.recv()
        if type(message) not in [str, bytes]:
            msgstr = json.dumps(message)
        else:
            msgstr = str(message)
        if config.get('echo', False):
            print(message.strip())
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
