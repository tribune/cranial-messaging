from collections import OrderedDict
import os
from typing import Any, Dict, Optional, Tuple  # noqa

immutable_config_keys = None  # type: Optional[Tuple[str]]
immutable_config_values = None  # type: Optional[Tuple[Any]]


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
    global immutable_config_keys
    global immutable_config_values
    if immutable_config_keys is not None:
        raise Exception('load() function should only be called once.')
    opts2env(opts, prefix)
    env = load_from_env(prefix)
    config = OrderedDict(env)
    immutable_config_keys = tuple(config.keys())  # type: ignore
    immutable_config_values = tuple(config.keys())  # type: ignore
    return env


def get():
    if immutable_config_keys is None:
        raise Exception('Config not yet loaded. Call load() instead.')
    return dict(zip(immutable_config_keys or tuple(),
                    immutable_config_values or tuple()))


if __name__ == "__main__":
    import doctest
    doctest.testmod()
