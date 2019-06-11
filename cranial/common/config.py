"""
This module exists to support the 12-factor App approach to configuration,
while also supporting convenient CLIs. This provides a subset of the
functionality of the `click` module, so you probably don't need this if you're
already using click.

CLI options take precendence over Environment Variables.

We assume, but do not enforce, the use of the `docopt` module to handle
configuration via command-line arguments. Any dict[str, str] of config values
will do, if you prefer to use lower level argument parsing.


Typical usage:
    1. Read command-line args first, e.g., `opts = docopt.docopt(__doc__)`
    2. Read and store environment variables, with
      `config.load(opts, 'some_env_var_prefix')`.
    3. Throughout the application, use `config.get()` to get a dict of
      configuration values.
"""

from collections import OrderedDict
import importlib
import os
from typing import Any, Dict, Optional, Tuple, Union  # noqa
import urllib.parse

import yaml

ConfigValue = Union[str, bool, Dict[str, Any]]
ConfigStore = Dict[str, ConfigValue]

immutable_config_values = None  # type: Optional[ConfigStore]


def opts2env(opts: Dict[str, str], prefix: str) -> None:
    """ Takes opts produced by docopt and puts then in Environment variables
    with the given case-insensitive prefix. `__name__` might make a good
    prefix.

    >>> opts2env({'--monty': 'spam', '<WITCH>': True, 'duck': False, 'a': 'b'},
    ...          'ctest')
    >>> os.environ['CTEST_MONTY']
    'spam'
    >>> os.environ['CTEST_WITCH']
    '1'
    >>> os.environ.get('CTEST_DUCK', 'missing')
    'missing'
    >>> os.environ['CTEST_A']
    'b'
    """
    prefix = prefix.upper()
    for k, v in opts.items():
        name = prefix + '_' + k.upper().strip('-<> ').replace('-', '_')
        if v is not False and v is not None:
            os.environ[name] = '1' if v is True else str(v)


def parse_uri(s: str) -> Dict:
    """Parse URIs into factory parameters."""
    parse = urllib.parse.urlparse(str(s))
    # Most of the Python ecosystem breaks if you name a module 'http',
    # so we use httpget.
    mod = 'httpget' if parse.scheme == 'http' else parse.scheme

    d = {'module': mod,
         'address': parse.netloc,
         'endpoint': parse.path[1:]}  # type: Dict[str, Any]
    if parse.username:
        d['user'] = parse.username
    if parse.password:
        d['password'] = parse.password
    if parse.query:
        q = urllib.parse.parse_qs(parse.query)
        d.update({k: i[0] if len(i) < 2 else i
                  for k, i in q.items()})
    return d


def load_from_env(prefix: str) -> ConfigStore:
    """ Returns a dict of environment variables with the given case-insensitive
    prefix.

    >>> os.environ['CTEST_FOO'] = '0'
    >>> os.environ['CTEST_BAR'] = 'hello'
    >>> os.environ['CTEST_URI'] = 'kafka://host/ok?mode=hot'
    >>> conf = load_from_env('CtEsT')
    >>> conf['FOO']
    False
    >>> conf['BAR']
    'hello'
    >>> conf['URI'] == dict(\
    module='kafka', address='host', endpoint='ok', mode='hot')
    True
    >>> conf['URI_STR']
    'kafka://host/ok?mode=hot'
    """
    prefix = prefix.upper()
    config = {}  # type: ConfigStore
    k = ''  # type: str
    v = ''  # type: ConfigValue
    for k, v in os.environ.items():
        if not k.startswith(prefix + '_'):
            continue
        # @TODO Not sure this is the best place for this.
        if type(v) is str and '://' in str(v):
            fullkey = k.upper() + '_STR'
            config[fullkey.replace(prefix+'_', '')] = v
            v = parse_uri(v)  # type: ignore
        elif type(v) is str:
            # Make falsey things actually False.
            v = False if str(v).lower() in ['0', 'false'] else v
        config[k.replace(prefix + '_', '')] = v
    return config


def load(opts: Dict[str, str], prefix: str, fname=None) -> ConfigStore:
    """
    >>> o = {}
    >>> o['--foo'] = '0'
    >>> o['--bar'] = 'hello'
    >>> o['--uri'] = 'kafka://host/ok?mode=hot'
    >>> conf = load(o, 'CteSt')
    >>> conf['FOO']
    False
    >>> conf['BAR']
    'hello'
    >>> conf['URI'] == dict(\
    module='kafka', address='host', endpoint='ok', mode='hot')
    True
    >>> os.environ['CTEST_URI']
    'kafka://host/ok?mode=hot'
    """
    global immutable_config_values
    if immutable_config_values is not None:
        raise Exception('load() function should only be called once.')
    conf = {k.upper(): v for k, v in parse_yaml_file(fname).items()} if fname \
        else {}
    str_conf = {k: v for k, v in conf.items() if type(v) is str}
    str_conf.update(opts)
    opts2env(str_conf, prefix)
    conf.update(load_from_env(prefix))
    immutable_config_values = OrderedDict(conf)
    return conf


def parse_yaml_file(fname: str) -> Dict[str, Any]:
    doc = open(fname, 'r')
    return yaml.full_load(doc)


def get(key=None, default=None) -> Union[ConfigStore, ConfigValue]:
    if immutable_config_values is None:
        raise Exception('Config not yet loaded. Call load() instead.')
    if key:
        val = immutable_config_values.get(key.upper(), default)
        return val.copy() if hasattr(val, 'copy') else val  # type: ignore
    else:
        return immutable_config_values.copy()


def factory(params: Dict) -> Any:
    if params.get('package'):
        params['module'] = '.'.join([params['package'], params['module']])
    mod = importlib.import_module(params['module'])
    cl = params['class']
    del(params['package'])
    del(params['module'])
    del(params['class'])
    return getattr(mod, cl)(**params)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
