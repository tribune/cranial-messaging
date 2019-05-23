import logging
import socket
from typing import Callable, List


def available_port():
    """ Not without race conditions; another process could claim the port
    between when you get the number and when you use it. This is intended for
    testing; production code should probably use known, fixed port numbers.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    addr, port = s.getsockname()
    s.close()
    return port


def update_ordered_list(old: List, new: List) -> List:
    """Returns a list of all items in new, retaining the order of old for the
    common items.

    >>> update_ordered_list([1,2,3], [4,3,1])
    [1, 3, 4]
    >>> update_ordered_list([1,2,3], [4,1,3])
    [1, 3, 4]
    >>> update_ordered_list([5,6,7], [4,1,3])
    [4, 1, 3]
    >>> update_ordered_list([6,5,7], [6,7,5])
    [6, 5, 7]
    """

    # Fast check.
    if sorted(old) == sorted(new):
        return old

    result = [x for x in old if x in new]
    for item in new:
        if item not in result:
            result.append(item)

    return result


def str2bucket(text: str, num_buckets: int) -> int:
    """Takes a string and uses a consistent hash to return an int selected
    evenly from the range 0 to (num_buckets - 1). Useful for 'sticky' load
    balancing on an arbitrary string key.

    >>> str2bucket('foo', 99)
    91
    >>> str2bucket('bar', 99)
    63
    >>> str2bucket('foo', 3)
    1
    >>> str2bucket('bar', 3)
    0
    >>> str2bucket('baz', 0)
    0
    """
    if num_buckets == 0:
        # Can't do modulo 0 or universe is destroyed.
        return 0
    from hashlib import md5
    return int(md5(text.encode()).hexdigest(), 16) % num_buckets


def dieIf(msg: str, fn: Callable, *args, **kwargs):
    """Wrap Exceptions in friendlier messages."""
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        logging.error('%s. Function: %s; Args: %s; Keywords: %s',
                      e, fn.__name__, args, kwargs)
        raise Exception(msg)


def warnIf(msg: str, fn: Callable, *args, **kwargs):
    """Transform exceptions into warnings."""
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        logging.warn(e)
        logging.warn('%s. Function: %s; Args: %s; Keywords: %s',
                     msg, fn.__name__, args, kwargs)
        raise e
        return None
