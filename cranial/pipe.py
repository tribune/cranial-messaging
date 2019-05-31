#! /usr/bin/python3
"""Usage: pipe.py [--debug] [--echo] [--response] [--update] [--include-empty]\
                  [--refresh <num>] [--key <key>] [--append <sep>]\
                  [--ext <str>] [--config <file>] [--list]\
                  [<listener>] [<target>]

Options:
  -e --echo                  Print messages received.
  -r --response              If the target responds, print it.
  -u --update                Given input & response dicts, echo combined.
  -i --include-empty         Pipe messages that include only whitespace.
  -t <num> --refresh <num>   Close & Recreate the connection to target after
                             this many messages. Use -t=10sec to refresh after
                             this many seconds instead.
  -k <key> --key <key>       If input is a dict, this is the key for the entry
                             containing the unique, primary key. [default: id]
  -a <sep> --append <sep>    If input is dict with an ID, append <sep> and the
                             ID to the end of the URI path.
  -x <str> --ext <str>       Suffix to append after ID when using --append.
  -f <file> --config <file>  Config file.
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

from time import sleep, time

from docopt import docopt
import ujson as json

import cranial.messaging  # noqa; For Typing.
import cranial.common.config as config
import cranial.common.logger as logger
from cranial.common.utils import dieIf

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
elif opts.get('<listener>') is None:
    print("At least a listener is required. Use for --help or --list to see " +
          "supported listeners & notifiers.")
    exit(1)

# ...and stdout
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


sleep_time = config.get('sleep', 1)

connect_time = time()
msg_count = 0
refresh = config.get('refresh')
by_time = refresh and refresh.endswith('sec')
refresh = refresh and int(refresh.replace('sec', ''))
last_id = int(config.get('listener', {}).get('last_id')
              or config.get('last_id', '0'))
target = None  # type: cranial.messaging.base.Notifier


while True:
    if refresh and not by_time and msg_count >= refresh:
        msg_count = 0
    if (msg_count == 0) \
            or (refresh and by_time and time() - connect_time > refresh):
        # This whole block of code has gotten ugly and needs refactoring.
        msg_count = 0
        send_params = config.get('target')
        notifier_str = config.get('target_str')
        if type(send_params) is str:
            # It's a filename
            send_params = {'package': 'cranial.messaging',
                           'module': 'file',
                           'class': 'Notifier',
                           'address': '',
                           'endpoint': send_params,
                           'path': send_params}
        sep = config.get('append')
        ext = ''
        if sep:
            ext = sep + str(last_id) + config.get('ext', '')
            send_params['endpoint'] += ext
            if send_params.get('path'):
                send_params['path'] += ext
        del(target)
        try:
            target = config.factory(
                {**send_params,
                 **{'package': 'cranial.messaging',
                               'class': 'Notifier'}})
        except ModuleNotFoundError:
            # Try unknown protocols through smart_open.
            send_params['module'] = 'file'
            send_params['path'] = notifier_str + ext
            target = dieIf("Target not properly configured", config.factory,
                           {**send_params,
                            **{'package': 'cranial.messaging',
                               'class': 'Notifier'}})

        connect_time = time()

    try:
        message = listener.recv()
        if type(message) not in [str, bytes]:
            msgstr = json.dumps(message, ensure_ascii=False)
        elif type(message) is bytes:
            msgstr = message.decode()
        else:
            msgstr = message

        if config.get('echo', False):
            print(msgstr.strip())
        if not config.get('include_empty') and msgstr.strip() == '':
            continue
    except StopIteration:
        break

    if message:
        msg_count += 1
        logging.debug('Received Message: %s', message)
        send_params['message'] = message
        # response = warnIf("Couldn't send", target.send, **send_params)
        response = target.send(**send_params)
        if response and config.get('response', False):
            print(str(response))

        if type(message) != dict:
            try:
                message = json.loads(response)
            except Exception as e:
                logging.info(e)
                pass
        if type(message) is dict:
            last_id = message.get(config.get('key', 'id')) or last_id
        elif type(message) != int:
            try:
                last_id = int(message.split(
                    config.get('delimiter', ','), 1)[0])
            except:  # noqa; We really do allow any exception here.
                pass
        else:
            last_id = message

        if config.get('update'):
            if type(response) != dict:
                try:
                    response = json.loads(response)
                except Exception as e:
                    logging.info(e)
                    pass

            if type(message) is dict and type(response) is dict:
                print(json.dumps({**message, **response}, ensure_ascii=False))

        sleep_count = 0
    else:
        sleep(sleep_time)
        sleep_count += 1
        if sleep_count % 5 == 0:
            logging.debug("No messages for %s seconds",
                          sleep_count * sleep_time)
