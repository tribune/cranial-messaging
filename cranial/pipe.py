#! /usr/bin/python3
"""Usage:
    pipe [--debug] [--echo] [--response] [--update] [--include-empty]
            [--refresh <num>] [--key <key>] [--separator <sep>]
            [--ext <str>] [--config <file>]
            [<listener>] [<target>]
    pipe [-erui] [-t <num>] [-k <key] [-s <sep>] [-x <str>] [-f file]
         [<listener>] [<target>]
    pipe --list
    pipe --help

Options:
  -e --echo                  Print messages received.
  -r --response              If the target responds, print it.
  -u --update                Given input & response dicts, echo combined.
  -i --include-empty         Pipe messages that include only whitespace.
  -t <num> --refresh <num>   Close & Recreate the connection to target after
                             this many messages. Use -t=10sec to refresh after
                             this many seconds instead.
  -k <key> --key <key>       If input is a dict, this is the key for the entry
                             containing the unique, primary key.
  -s <sep> --separator <sep> If input is dict with an ID, append <sep> and the
                             ID to the end of the URI path.
  -x <str> --ext <str>       Suffix to append after ID when using -s.
  -f <file> --config <file>  Config file. [default: pipe.yml]
  -l --list                  List supported protocols & exit.

Usage examples:

  $ echo "hello world" | cranial pipe stdin:// file://./out.txt

  $ cranial pipe out.txt stdout://  #URI protocol is optional for files.

  $ cranial pipe -re out.txt http://httpbin.org/anything

  $ echo "- also means stdin" | cranial pipe -r - httppost://httpbin.org/post

  $ cranial pipe kafka://broker.a,broker.b/topic # stdout is the default

  $ cranial pipe postgresql://your.host:5439/name/table?last_id=0 \
  >  ssh://you@example.com:22022/file.json.bzip2

  $ cranial pipe db://your.host/name/table?driver=mysql \
  >  hdfs://example.com/path/to/file.json.gz

  $ cranial pipe tweets://yourname:password@#someTag \
  >   fb://yourname:password@ # Doesn't exist yet, but Easy to implement.

  $ cranial pipe -r out.txt http://httpbin.org/anything | cranial pipe -\
  >   s3://bucket/piping-to-myself/responses.txt.gz

@TODO
Config Example:
listener: module=stdin
target: module=httpget address=localhost:8000 endpoint=hello
sleep: 10
"""

import os
from time import sleep, time
from typing import Callable, Dict, List, Optional, Tuple  # noqa

from docopt import docopt
from recordclass import RecordClass
import ujson as json

import cranial.messaging  # noqa; For Typing.
from cranial.messaging.base import Message, Notifier
import cranial.common.config as config
import cranial.common.logger as logger
from cranial.common.utils import dieIf, warnIf

logging = logger.get()

opts = docopt(__doc__)

if opts.get('--list'):
    import pkgutil
    import cranial.listeners as L
    import cranial.messaging as N
    print('Built-in Protocols\n==================')
    for pkg, name in [(L, "Listeners"), (N, "Notifiers")]:
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

# Conventional syntax for stdin
if opts.get('<listener>') == '-':
    opts['<listener>'] = 'stdin://'
elif opts.get('<listener>') is None and not opts.get('--config'):
    print("At least a listener is required. Use for --help or --list to see " +
          "supported listeners & notifiers.")
    exit(1)

# ...and stdout
if opts.get('<target>') == '-':
    opts['<target>'] = 'stdout://'

if os.path.isfile('pipe.yml') or opts.get('--config') != 'pipe.yml':
    dieIf("Couldn't load config", config.load,
          opts, prefix='cranial_pipe', fname=opts['--config'])
else:
    dieIf("Couldn't load config", config.load,
          opts, prefix='cranial_pipe')


if config.get('debug'):
    logging.setLevel('DEBUG')
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


class NotifierTracker(RecordClass):
    target: Optional[Notifier]
    builder: Callable
    msg_count: int
    connect_time: float
    last_id: int = 0


# ------------------[Notifier, Params, num messages, connect time]
NotifierQuad = Tuple[Notifier, Dict,   int,          float]
NOTIFIER_PARAMS = {'package': 'cranial.messaging', 'class': 'Notifier'}


def target_builder(params: Dict,
                   uri: str = ''
                   ) -> Callable[[Notifier, int, float, int], NotifierQuad]:
    refresh = params.get('refresh') or config.get('refresh')
    by_time = refresh and refresh.endswith('sec')
    refresh = refresh and int(refresh.replace('sec', ''))
    if type(params) is str:
        # It's a filename
        params = {'module': 'file',
                  'address': '',
                  'endpoint': params,
                  'path': params,
                  **NOTIFIER_PARAMS}
    sep = params.get('separator') or config.get('separator', '')
    extfmt = '{}' + sep + '{}' + (params.get('ext') or config.get('ext', ''))

    try:
        config.factory({**params, **NOTIFIER_PARAMS})
    except ModuleNotFoundError:
        # Try unknown protocols through smart_open.
        params['module'] = 'file'
        params['path'] = uri

    orig_endpoint = params.get('endpoint', '')
    orig_path = params.get('path', '')

    def get_target(target: Optional[Notifier],
                   msg_count: int,
                   connect_time: float,
                   last_id: int) -> NotifierQuad:

        if refresh and not by_time and msg_count >= refresh:
            msg_count = 0

        if (msg_count == 0) \
                or (refresh and by_time and time() - connect_time > refresh):
            if sep:
                params['endpoint'] = extfmt.format(orig_endpoint, last_id)
                if params.get('path'):
                    params['path'] = extfmt.format(orig_path, last_id)

            target = dieIf(
                "Couldn't build Target",
                config.factory,
                {**params, **NOTIFIER_PARAMS})

            return target, params, 0, time()
        else:
            return target, params, msg_count, connect_time

    return get_target


def message_update(message: Message, response: Message) -> Message:
    try:
        response = response.dict()
    except (TypeError, ValueError):
        response = {"response": response.str()}

    try:
        message = message.dict()
    except (TypeError, ValueError):
        message = {"message": message.str()}

    return Message({**message, **response})


sleep_time = config.get('sleep', 1)

try:
    last_id = int(config.get('listener', {}).get('last_id', 0))
except AttributeError:
    last_id = int(config.get('last_id', 0))

now = time()
pipeline = []  # type: List[NotifierTracker]
for p in config.get('pipeline', []):
    uri = ''
    if isinstance(p, str):
        uri = p
        p = config.parse_uri(p)
    pipeline.append(NotifierTracker(
        None, target_builder(p, uri), 0, now, last_id))

params = config.get('target', {'module': 'stdout'})  # type: Dict
get_target = target_builder(params, config.get('target_str'))
pipeline.append(NotifierTracker(None, get_target, 0, now, last_id))


# @TODO Use importlib to config by string
serde = json

if config.get('debug'):
    timer = time()
while True:  # noqa
    try:
        message = Message(listener.recv(), serde=serde)

        if config.get('echo', False):
            print(message.str().strip())
        if not config.get('include_empty') and message.str().strip() == '':
            continue
    except StopIteration:
        break

    if message.raw:
        logging.debug('Received Message: %s', message)

        # Sending...
        for nt in pipeline:  # type: NotifierTracker
            nt.target, params, nt.msg_count, nt.connect_time = nt.builder(
                nt.target, nt.msg_count, nt.connect_time, nt.last_id)
            nt.msg_count += 1
            params['message'] = message.str()
            response = warnIf("Couldn't send", nt.target.send, **params)
            if response and config.get('response', False):
                print(response)

            try:
                # @TODO this is probably slowing us down unnecessarly, and it's
                # ugly.
                nt.last_id = message.dict().get(
                    config.get('key', 'id')) or nt.last_id
            except (TypeError, ValueError) as e:
                # Message is probably not converatble to a dict.
                if config.get('debug'):
                    print(e)
                pass

            if response and config.get('update'):
                message = message_update(message, Message(response))
            elif response:
                message = Message(response)

        if config.get('update') and config.get('response'):
            print(message.str())

        # End sending.
        sleep_count = 0
    else:
        sleep(sleep_time)
        sleep_count += 1
        if sleep_count % 5 == 0:
            logging.debug("No messages for %s seconds",
                          sleep_count * sleep_time)

if config.get('debug'):
    print('Loop time: {}'.format(time() - timer))
