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
import os
from typing import Any, Dict, Optional, Tuple  # noqa

import yaml

immutable_config_values = None  # type: Optional[Dict[str, Any]]


def opts2env(opts: Dict[str, str], prefix: str) -> None:
    """ Takes opts produced by docopt and puts then in Environment variables
    with the given case-insensitive prefix. `__name__` might make a good
    prefix.

    >>> opts2env({'--monty': 'spam', 'WITCH': True, 'duck': False}, 'dse_test')
    >>> os.environ['DSE_TEST_MONTY']
    'spam'
    >>> os.environ['DSE_TEST_WITCH']
    '1'
    >>> os.environ.get('DSE_TEST_DUCK', 'missing')
    'missing'
    """
    prefix = prefix.upper()
    for k, v in opts.items():
        name = prefix + '_' + k.upper().replace('--', '').replace('-', '_')
        if v is not False and v is not None:
            os.environ[name] = '1' if v is True else str(v)


def load_from_env(prefix: str):
    """ Returns a dict of environment variables with the given case-insensitive
    prefix.

    >>> os.environ['DSE_LOAD_TEST_FOO'] = '0'
    >>> os.environ['DSE_LOAD_TEST_BAR'] = 'hello'
    >>> load_config_from_env('dse_LOAD_test') == {'FOO': False, 'BAR': 'hello'}
    True
    """
    prefix = prefix.upper()
    config = {}
    for k, v in os.environ.items():
        if not k.startswith(prefix + '_'):
            continue
        # Make falsey things actually False.
        v = False if v.lower() in ['0', 'false'] else v  # type: ignore
        config[k.replace(prefix + '_', '')] = v
    return config


def load(opts: Dict[str, str], prefix: str):
    global immutable_config_values
    if immutable_config_values is not None:
        raise Exception('load() function should only be called once.')
    opts2env(opts, prefix)
    env = load_from_env(prefix)
    immutable_config_values = OrderedDict(env)
    return env


def parse_yaml_file(fname: str):
    doc = open(fname, 'r')
    return yaml.load(doc)


def get(key=None):
    if immutable_config_values is None:
        raise Exception('Config not yet loaded. Call load() instead.')
    if key:
        val = immutable_config_values[key]
        return val.copy() if hasattr(val, 'copy') else val
    else:
        return immutable_config_values.copy()


if __name__ == "__main__":
    import doctest
    doctest.testmod()
