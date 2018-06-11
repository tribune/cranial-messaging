# encoding: utf-8
"""
Helper module for example applications. Mimics ZeroMQ Guide's zhelpers.h.
"""
from __future__ import print_function

import binascii
import os
from random import randint
from time import time

import zmq

from cranial.common import logger

log = logger.get('ZMQ_LOGLEVEL')


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


def zpipe(ctx):
    """build inproc pipe for talking to threads

    mimic pipe used in czmq zthread_fork.

    Returns a pair of PAIRs connected via inproc
    """
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a, b


def send_string(s: str, host: str, wait=True) -> str:
    context = zmq.Context.instance()
    client = context.socket(zmq.REQ)
    set_id(client)
    client.connect("tcp://" + host)

    msg = bytes(s, 'ascii')
    client.send(msg)
    response = ''
    if wait:
        response = client.recv().decode('ascii', 'ignore')
    return response


def get_client(port=5678, host='localhost'):
    context = zmq.Context.instance()
    client = context.socket(zmq.REQ)
    set_id(client)
    client.connect("tcp://{}:{}".format(host, port))
    return client


def client_send_request(client, msg) -> str:
    """Probably you should be using zmq_send_string() instead of this
    function."""
    msg = msg if type(msg) is bytes else bytes(msg, 'ascii')
    client.send(msg)
    start = time()
    resp = client.recv()
    end = time()
    log.debug(resp.decode('ascii'))

    t = end - start
    log.debug("Time: {:.3f}".format(t))

    return resp.decode('ascii')
