"""
Helper module for example applications. Mimics ZeroMQ Guide's zhelpers.h.
"""

import binascii
import os
from random import randint
from time import time

import zmq

from cranial.common import logger

log = logger.get('ZMQ_LOGLEVEL')
default_context = zmq.Context.instance()


def socket_set_hwm(socket, hwm=-1):
    """libzmq 2/3/4 compatible sethwm"""
    try:
        socket.sndhwm = socket.rcvhwm = hwm
    except AttributeError:
        socket.hwm = hwm


def dump(msg_or_socket):
    """Receives all message parts from socket, printing each frame neatly"""
    if isinstance(msg_or_socket, zmq.Socket):
        # it's a socket, call on current message
        msg = msg_or_socket.recv_multipart()
    else:
        msg = msg_or_socket
    print("----------------------------------------")
    for part in msg:
        print("[%03d]" % len(part), end=' ')
        try:
            print(part.decode('ascii'))
        except UnicodeDecodeError:
            print(r"0x%s" % (binascii.hexlify(part).decode('ascii')))


def set_id(zsocket):
    """Set simple random printable identity on socket"""
    identity = u"%04x-%04x" % (randint(0, 0x10000), randint(0, 0x10000))
    zsocket.setsockopt_string(zmq.IDENTITY, identity)
    return identity


def zpipe(ctx=None):
    """build inproc pipe for talking to threads

    mimic pipe used in czmq zthread_fork.

    Returns a pair of PAIRs connected via inproc
    """
    ctx = ctx or default_context
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a, b


def send_string(s: str, host: str,
                wait=True, encoding='utf-8', ctx=None) -> str:
    """
    >>> from cranial.listeners.zmq import Listener
    >>> from cranial.listeners.base import echo_process
    >>> addr = echo_process(Listener, b'Bar')
    >>> assert(send_string('Foo', addr, wait=False) == '')
    >>> send_string('Foo', addr)
    'FooBar'
    >>> res = [str(i) + 'Bar' for i in range(0,9)]
    >>> assert(res == [send_string(str(i), addr) for i in range(0,9)])
    >>> assert(send_string('exit', addr, wait=False) == '')
    """
    context = ctx or default_context
    client = context.socket(zmq.REQ)
    set_id(client)
    client.connect("tcp://" + host)

    msg = s if type(s) is bytes else bytes(s, encoding)
    client.send(msg)
    if wait:
        return client.recv().decode(encoding, 'replace')
    else:
        return ''


def get_client(port=5678, host='localhost', ctx=None):
    context = ctx or default_context
    client = context.socket(zmq.REQ)
    set_id(client)
    client.connect("tcp://{}:{}".format(host, port))
    return client


def client_send_request(client, msg) -> bytes:
    """Probably you should be using send_string() instead of this
    function."""
    msg = msg if type(msg) is bytes else bytes(msg, 'utf-8')
    client.send(msg)
    start = time()
    resp = client.recv()
    end = time()
    log.debug(resp.decode('utf-8', 'replace'))

    t = end - start
    log.debug("Time: {:.3f}".format(t))

    return resp


if __name__ == "__main__":
    import doctest
    doctest.testmod()
