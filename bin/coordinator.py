#!python -o
"""
    Cranial Coordinator

    Inspired by the Kafka Group Manager, though not attempting to be a direct port:
    https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala  # noqa

    Goal:
    If you have a reliable producer and a reliable destination, then we provide
    at-least-once guaranteed processing for all messages across a moderate number
    of unreliable workers with target efficiency equal to Kafka.
    Exactly-once processing is possible by check-pointing with the destination.

    efficiency = cost per 1000 messages per second
        incl. Workers & at least 3 Brokers.

    Priorities, in Order:
        1. Reliablity
        2. Prevent Duplication
        3. Speed
        4. Utilization of Workers
        5. Readable Code

        That is, we should strive to maximize all of these things, up until
        doing so has a negative impact on the higher priority thing.

        Why not Raft Consensus?
        --------------
        In fact, the store should use a Consensus algorithm like Raft to ensure
        eventual consistency, but Raft has a couple requirements we want to
        stretch:
            1. Raft assumes that once a member joins the cluster, it intends to
               sitck around more or less permanently. Instead, our worker groups
               are expected to grow and shink constantly as data bandwidth and
               worker availability fluctuates.
            2. In the case of a Network split, under Raft, one side of the
               split will stop writing to the Log. We prefer to err on the side
               of continuing to process, at the risk of possibly duplicated
               processing or lower performance.
"""
import asyncio
import logging
from time import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union
from uuid import uuid4

from aioetcd3.client import client, Client
from aioetcd3.kv import KVMetadata
from enum import Enum
from recordclass import RecordClass
from toolz import memoize

from cranial.listeners.base import Listener

# type aliases
MsgId = int
Msg = Any
Partition = str
Checkpoint = Optional[int]
TotalParts = int
kvResult = Tuple[bytes, bytes, KVMetadata]


# Constants for kvResult items.
KEY = 0
VALUE = 1
META = 2

# General Constants for Lists.
FIRST = 0
LAST = -1

starttime = None
PREFIX = '/cc/'
GROUP_ACK_PATH_3 = 'ack/{}/group/{}/{}'  # requestor, revision, part
CHECKIN = 1  # Every n seconds

LOGLEVEL = 'INFO'
logging.basicConfig(level=LOGLEVEL)


class Response(Enum):
    EMPTY = b''
    OK = b'1'
    DENY = b'0'


class PendingRequest(RecordClass):
    requestor: str
    recipient: str
    revision: int
    response: Optional[asyncio.Future] = None
    part: Optional[bytes] = None  # The granted part, once responded.


class PendingGroupRequest(PendingRequest):
    pass


@memoize
def path(key: str, *args, prefix=PREFIX):
    return _path(key, *args, prefix=prefix)


def _path(key: str, *args, prefix=PREFIX):
    """
    >>> _path('k', prefix='test')
    'testk'
    >>> _path('a', 'b', 'c', prefix='test/')
    'test/a/b/c'
    >>> _path('test/a', prefix='test/')
    'test/a'
    """
    return ((prefix if not key.startswith(prefix) else '')
            + key
            + ('/' if args else '')
            + '/'.join([str(a) for a in args]))


@memoize
def span(key: str):
    return _span(key)


def _span(key: str):
    """A Range from 'key/' to 'key0'

    >>> span('foo')
    ('/cc/foo/', '/cc/foo0')
    """
    return (path(key + '/'), path(key + "0"))


@memoize
def kd(keys, prefix=PREFIX):
    """ Memoized Convenience alias. """
    return keys_to_dict(keys, prefix)


def keys_to_dict(
        keys: List[Tuple], prefix=PREFIX) -> Dict[str, Union[Dict, str]]:
    """
    >>> keys = [(b'/foo', b'1', None), (b'/foo/bar/baz', b'2', None)]
    >>> keys_to_dict(keys, '/')
    {'foo': {'value': b'1', 'bar': {'value': None, 'baz': {'value': b'2'}}}}
    """
    d = {}  # type: Dict[str, Any]
    for k in keys:
        name, value, meta = k
        # Strip the prefix.
        name = name[len(prefix):]
        units = name.decode().split('/')
        mut = d
        for u in units:
            # We might have different values for '/foo' and '/foo/bar'
            if u not in mut:
                mut[u] = {'value': None}
            mut = mut[u]
        mut['value'] = value
    return d


def cranial_producer(l: Listener) -> Iterator[Tuple[MsgId, Msg]]:
    id = 0
    while True:
        msg = l.recv()
        yield id, msg
        id += 1
        if hasattr(l, 'resp'):
            l.resp('OK')


async def get_single_value(cl: Client, key: str) -> bytes:
    """
    >>> from cranial.messaging.test.test_coordinator import _fake_client
    >>> c = run(_fake_client(PREFIX).preload())
    >>> run(get_single_value(c, 'parts/total'))
    b'7'
    """
    if not key.startswith(PREFIX):
        key = path(key)
    results = await cl.range(key)
    logging.debug('Got raw %s: %s', key, results)
    if len(results) < 1:
        return Response.EMPTY  # type: ignore
    if len(results) > 1:
        logging.warning('More than one result for range %s', key)
    return results[0][VALUE]


async def init(cl: Client, num_parts: int = 6):
    """
    >>> from cranial.messaging.test.test_coordinator import *
    >>> cl = FakeClient()
    >>> run(init(cl, 5))
    >>> run(get_single_value(cl, 'parts/total'))
    b'5'
    >>> len(run(cl.range(span('parts/unassigned'))))
    5
    >>> run(init(cl))
    Traceback (most recent call last):
    ...
    Exception: Store already initialized.
    """
    # @TODO All these should be in a transaction.
    if await get_single_value(cl, 'init') == b'1':
        raise Exception('Store already initialized.')
    else:
        # @TODO parallelize
        await cl.put(path('init'), '1')
        await cl.put(path('parts/total'), str(num_parts))
        [await cl.put(path('parts/unassigned', n), '0')
            for n in range(num_parts)]
    logging.debug('Store on init: %s', cl.store)


async def get_workers(cl: Client) -> List[kvResult]:
    return await cl.range(span('workers'))

"""
  The protocol for getting partitions:

  1. New worker asks the store for list of workers and their partitions.

     Q. What is the store?
     A. A distributed, strongly-consistent (but not necessarily
     strictly-consitent) data storage system which can run as a side-car in
     the same OS environment as each worker. By default, we use etcd. The
     core requirement is eventual consistency, with each value being stored
     with a unique incrementing ID on each revision.

     Q. What if the store is out of date when a worker reads from it?
     A. It doesn't matter, because if New worker asks another worker for a
     a partition that the worker no longer owns, it will deny the request or
     not answer.

  2. New worker identifies "over-worked" workers and writes to store a
  Request to to Old worker to take over a partition.

     Q. What is an over-worked worker?
     A. A worker that owns more than a fair share of partitions.

     Q. What is a fair share of partitions?
     A. int(num_partitions / num_workers)

     Q. What if one worker is very over-worked?
     A. New worker will order the workers it requests from by the number of
        partitions it believes the worker currently owns to reduce the
        burden on the most overworked workers first.

     Q. What if all partitions are already distributed fairly?
     A. They won't be as soon as the New worker joins. If there are n * 2
        partitions currently distibuted among n previous workers, then the
        new fairness threshold is floor((n * 2)/(n+1)) -> 1 and so the
        New worker will request one of the partitions owned by the first
        known worker.

     Q. What if there are more workers than partitions?
     A. The the New worker will sit around waiting for Expired Partitions.
        (see below)

  3. Old worker sees the request. If Old worker still owns the partition,
  it stops processing the partition, then checkpoints the last ID
  processed, then removes from store it's claim on the partition,
  writes to store an Ack for New worker, and deletes the request.
  If not, Old worker can optionally write a Deny message that New worker
  can optionally read in order to cancel it's request.

     Q. How does a worker "Checkpoint" an ID.
     A. Ideally, it atomically writes the (Partition ID, Message ID) pair to
        the same destination in the same transaction as it writes the
        processed messages, in a way that is reliably and quickly queriable.
        This is necessary for exactly-once processing.
        If not possible, it can write to the store. This does allow the small
        probability that a message will be processed twice if a worker
        crashes between when it writes the processed message and when it
        writes the checkpoint.

     Q. What if the Old worker never Acks because it's busy or dead?
     A. The the partition remains in the store as assigned to the Old worker.
     Periodically, a well-behaved worker will get the checkpoints for all
     partitions. Any Expired partitions can be requested with a special
     Group Request. Every well-behaved worker will read all Group Requests,
     and Ack if it agrees with the New worker about the Expired status of
     the partition and it does not have it's own outstanding request for the
     partition with a lower revision number. The New worker may claim the
     Group Requested partition and delete the former owner's claim on
     the partition when all Live workers have Ack'd the Group Request, or
     when it has been the only Live worker for $Z seconds. If this leaves
     the former owner with 0 claimed partitions, the new worker may
     delete the former owner's registration.
     If a worker ever ses that it's own Group Request has a revision number
     higher than the revision of another request for the a same partition, it
     should delete it's own request (and Ack the other one).

     Q. Is it possible to have a Deadlock where an Expired Partition is never
        claimed?
     A. Every well-behaved worker will eventually Ack the group request,
        unless it is the owner of the requested partition itself, in which
        case the partition has been successfuly claimed, or else the worker
        is unable to communicate with the group, in which case, it's
        registration will expire, and it will no longer be counted among
        those required to Ack the request.

     Q. How do we prevent duplicate processing in case of split-brain where
        two parts of the group continue working but can't communicate?
     A. If checkpoints are not being written to the destination, this may be
     unavoidable. In that case, users would need to a method to deduplicate
     messages in the destination system. Otherwise, if a worker claims an
     Expired partition, it should put itself in "strict mode" for that
     partition. In this mode, the Worker should use a Compare-and-Swap
     operation so that the processed message is written atomically to the
     destination along with the new Checkpoint only if the destination has
     the expected previous Checkpoint. If the Worker finds a different
     checkpoint, it may pause the partition for some period of time.
     This prevents duplication, potentially at the cost of a significant
     performance penalty.
     If either of the split-brain groups is unable to write to the
     destination, then the issue is moot, as duplicated messages won't be
     written. A well-behaved worker that finds that it cannot write to the
     destination should delete its registration and unassign its
     partitions after some reasonable period of failed attempts.

     Q. What is a paused partition?
     A. A partition written to the store in some way to indicate that its
     owner has paused it. A worker should Deny
     requests or Group requests for a partition it knows to be paused, and
     does not process messages for the partition. A worker can delete the
     pause of a partition it owns if it sees that another worker has claimed
     the partition after deleting it's own claim. (This may occur if workers
     rejoin the group after a split-brain scenario.)

     Q. What is an Expired Partition?
     A. A partitions for which the last checkpointed ID is more than $N
     values behind the last ID the worker has seen, or is timestamped
     more than $X seconds ago, depending on the configration of the workers.
     A worker that succesfully claims an Expired Partition should delete its
     pause, if any exists.

     Q. What is a Live Worker?
     A. A worker that is registered and owns at least one partition and
        no Expired Partitions.

     Q. How does a worker Register?
     A. A worker registers by writings it's UUID to the store. It should do
     so when it starts, and periodially as a heartbeat. Any worker can delete
     the registration of another worker that has not updated its registration
     in the past $Y seconds.

  4. New worker sees the Ack, writes to the store it's claim on the
  partition, and then starts processing messages for the partition.

     Q. Can two different workers claim the same partition at the same time?
     A. No. A well-behaved worker will only claim a partition if it has
        received an Ack from the previous partition owner, and an owner will
        only Ack the first request it sees.

     Q. Is it possible to change the number of partitions?
     A. Not currently; doing so will require all workers to momentarily pause
     and agree on the new number of partitions before proceeding.

  Steps 2-4 are repeated until New worker has claimed a fair number of
  partitions. It can do this in parallel with other requests, and
  asynchronously while it processes messages for partitions it has claimed.
"""


async def fair(cl, num_parts: int) -> int:
    workers = await get_workers(cl)
    num_workers = len(workers) or 1
    return int(num_parts / num_workers)


async def get_parts_with_total(cl) -> Tuple[List[kvResult], int]:
    """ @TODO tests """
    parts = await cl.range(span('parts'))
    total = None
    logging.debug('Parts: %s', parts)
    for p in parts:
        if p[0] == bytes(path('parts/total'), 'utf-8'):
            total = int(p[1].decode())
            break
    if not total:
        raise Exception("Couldn't get total number of partitions")

    return parts, total


async def req_partition_from(
        cl, requestor: str, recp: str, rev: int) -> PendingRequest:
    key = path('req', recp)
    value = '{},{}'.format(requestor, rev)
    logging.debug('Putting %s: %s', key, value)
    await cl.put(key, value)
    p = PendingRequest(requestor, recp, rev)
    # To trigger the first check for a response...
    await confirm_single_request(cl, p)
    return p


async def req_partition_from_group(
        cl, requestor: str, part: str, rev: int
) -> Optional[PendingGroupRequest]:
    key = path('group-req', part)
    # Don't make a new request if one already exists.
    if await get_single_value(cl, key) != Response.EMPTY:
        return None
    key += '/' + requestor

    value = rev
    logging.info('Putting %s: %s', key, value)
    await cl.put(key, value)
    req = PendingGroupRequest(requestor, key, rev, part=part)
    # To trigger the first check for a response...
    await confirm_group_request(cl, req)
    return req


async def delete_pending(cl, p):
    if type(p) is PendingGroupRequest:
        # First delete the request so no one else tries to respond,
        key = group_req_key(p)
        await cl.delete(key)
        # Then delete any respones received so far.
        key = 'ack/{}/group/{}/{}'.format(p.requestor, p.revision, p.part)
        for resp in await cl.range(span(key)):
            schedule(cl.delete(path(resp[KEY])))
    else:
        await cl.delete(path('req', p.recipient))


def key_tail(r: kvResult) -> str:
    return r[KEY].decode().split('/')[LAST]


def group_req_key(p: PendingGroupRequest) -> str:
    return path('group-req', p.part, p.requestor)


async def confirm_single_request(cl, p: PendingRequest) -> Response:
    """ Has p returned a matching Ack yet?
    Mutates p.
    @TODO Tests.
    """
    logging.debug('Confirming: %s', p)
    key = 'ack/{}/{}'.format(p.requestor, p.recipient)
    if p.response and p.response.done():
        result = p.response.result()
        if result == Response.EMPTY:
            # Nothing from the old owner yet. Try again:
            p.response = schedule(get_single_value(cl, key))
            return Response.EMPTY

        confirm, p.part = result.decode().split(',')
        logging.info('Got %s: %s =?= %s', key, confirm, p.revision)

        if confirm == str(p.revision):
            schedule(cl.delete(key))
            return Response.OK
        else:
            raise Exception("Mismatched Revision for Reqest Ack: %s", key)

    # We don't have a response yet.
    if not p.response:
        logging.debug('Not yet responded: %s', p)
        p.response = schedule(get_single_value(cl, key))

    # We've asked for response but haven't heard back yet.
    return Response.EMPTY


async def worker_has_expired_part(cl, worker_id) -> bool:
    # @TODO Implement.
    return False


async def unassign_worker_parts(cl, worker_id) -> None:
    result = await cl.range(path('parts', worker_id))
    if len(result):
        _, parts, rev = result[FIRST]
        for p in parts.decode().split(','):
            schedule(cl.put(path('parts/unassigned', p), str(rev)))


async def worker_is_dead(cl, worker_id: str) -> bool:
    key = path('workers', worker_id)
    timeout = await get_single_value(cl, key)

    if timeout == Response.EMPTY:
        return True

    if time() > float(timeout) or await worker_has_expired_part(cl, worker_id):
        schedule(cl.delete(key))
        schedule(unassign_worker_parts(cl, worker_id))
        return True

    return False


async def check_all_responses(
        cl: Client, p: PendingGroupRequest, key: str) -> Response:
    """
    Params
    ------
    key
    The key prefix where responses are colected.
    """
    responses = {key_tail(r): r for r in p.response.result()}  # type: ignore
    # To be followed by reponder's id.
    workers = await get_workers(cl)
    for w in workers:
        # Do we have an Ack?
        worker_id = w[KEY].decode().split('/')[-2]
        if worker_id == p.requestor:
            # A requestor doesn't respond to their own requests.
            continue
        ack = responses.get(
            worker_id, tuple([None, Response.EMPTY]))[VALUE]
        if ack == Response.DENY:
            return ack
        if ack == Response.EMPTY:
            if not await worker_is_dead(cl, worker_id):
                # It's not dead, so try again soon.
                p.response = schedule(cl.range(span(key)))
                return ack
            else:
                # It's dead, so ignore it.
                logging.warning("%s sees %s as DEAD!", p.requestor, worker_id)
                continue

    return Response.OK


async def confirm_group_request(cl, p: PendingGroupRequest) -> Response:
    """ Has p returned matching Acks from all live workers?
    Mutates p.
    @TODO Tests.
    """
    logging.debug('Confirming: %s', p)
    key = 'ack/{}/group/{}/{}'.format(p.requestor, p.revision, p.part)

    if p.response and p.response.done():
        # Responses indexed by worker id.
        ack = await check_all_responses(cl, p, key)
        if ack != Response.OK:
            return ack
        # If we haven't returned yet, then every worker has OK'd!
        try:
            # Using try here is mainly for testing or theoretiical future
            # stores; etcd will happily accept a delete request for a key that
            # doesn't exist.
            await cl.delete(path('unassigned', p.part))
        except KeyError:
            pass
        for r in p.response:
            schedule(cl.delete(r[KEY]))
        schedule(cl.delete(group_req_key(p)))
        return Response.OK

    # We haven't made the request yet.
    if not p.response:
        p.response = schedule(cl.range(span(key)))

    # We've asked for response but haven't heard back yet.
    return Response.EMPTY


async def confirm_pending_requests(cl, pending: List) -> List[Partition]:
    """ Mutates pending. """
    acks = []  # type: List[Partition]
    for req in pending:
        resp = await confirm_request(cl, req)
        if resp == Response.OK:
            logging.info(
                '%s Got Ack for %s from %s',
                req.requestor, req.part, req.recipient)  # type: ignore
            acks.append(req.part)
            pending.remove(req)
        elif resp == Response.DENY:
            logging.info(
                '%s Got Deny for %s from %s',
                req.requestor, req.part, req.recipient)  # type: ignore
            pending.remove(req)
    return acks


async def confirm_request(cl, p) -> Response:
    if type(p) is PendingRequest:
        return await confirm_single_request(cl, p)
    elif type(p) is PendingGroupRequest:
        return await confirm_group_request(cl, p)
    else:
        raise TypeError('Not a Pending Request Type.')

    # This should never happen.
    return Response.DENY


def get_available_parts(
        parts: List[kvResult], num_fair: int, myid: str) -> Tuple[List, bytes]:
    workers = filter(
        lambda x: b'/unassigned/' not in x[KEY], parts)

    overworkers = []
    myparts = b''
    for w in workers:
        if w[KEY].decode().endswith(myid):
            myparts = w[VALUE]
            continue
        partitions = w[VALUE].decode().split(',')
        num = len(partitions)
        rev = w[META].mod_revision  # type: ignore
        if num > num_fair:
            overworkers.append((w[KEY], num, rev))

    return overworkers, myparts


async def req_unassigned_parts(
        cl: Client, myid: str, parts: List[kvResult], how_many: int
) -> List[PendingGroupRequest]:
    """
    >>> from cranial.messaging.test.test_coordinator import _fake_client
    >>> c = run(_fake_client(PREFIX).preload())
    >>> parts, total = run(get_parts_with_total(c))
    >>> run(req_unassigned_parts(c, 'foo', parts, 99))[0].recipient
    '/cc/group-req/6/foo'
    """
    unassigned = [p for p in parts if b'/unassigned/' in p[KEY]]
    logging.debug('Unassigned parts for %s: %s', how_many, unassigned)

    pending = []  # type: List[PendingGroupRequest]
    while len(pending) < how_many and len(unassigned):
        p = unassigned.pop()
        req = await req_partition_from_group(
            cl,
            myid,
            key_tail(p),
            p[META].mod_revision)  # type: ignore
        logging.debug(req)
        if req:
            pending.append(req)

    return pending


async def req_overworked_parts(
        cl, myid, overworkers, how_many
) -> List[PendingRequest]:

    pending = []  # type: List[PendingRequest]
    # Sorted by workers having the most parts assigned.
    for w in sorted(overworkers, key=lambda x: -x[VALUE]):
        logging.debug('Worker: %s', w)
        name = w[KEY].decode().split('/')[LAST]
        rev = w[META]
        if len(pending) < how_many:
            # @TODO Only send request if there isn't another outstanding
            # recent request from another worker for the same part.
            logging.info('%s Requesting parts from %s',
                         myid, name)
            pending.append(
                await req_partition_from(cl, myid, name, rev))
            logging.debug('Pending: %s', pending)
        else:
            break

    return pending


async def req_parts(cl: Client,
                    myid: str,
                    pending: List[PendingRequest] = None
                    ) -> Tuple[Dict[Partition, Checkpoint], TotalParts]:
    """
    Mutates `pending`.

    Joining a preexisting cluster:
    >>> from cranial.messaging.test.test_coordinator import _fake_client
    >>> c = run(_fake_client(PREFIX).preload())
    >>> run(req_parts(c, 'test')) == ({'1': 42, '6': None}, 7)
    True

    Starting a new cluster:
    >>> c = _fake_client(PREFIX)
    >>> run(init(c, 3))
    >>> run(req_parts(c, 'test2')) == ({'0': None, '1': None, '2': None}, 3)
    True
    """
    pending = pending or []
    parts, total = await get_parts_with_total(cl)
    num_fair = await fair(cl, total)

    if len(pending) < num_fair:
        # First look for unassigned parts.
        pending.extend(
            await req_unassigned_parts(
                cl, myid, parts, num_fair - len(pending)))

    myparts = None
    if len(pending) < num_fair:
        overworkers, myparts = get_available_parts(parts, num_fair, myid)

        # No available partitions. @TODO Ask for partition increase.
        if len(overworkers) == 0 and len(pending) == 0:
            logging.info('No available partitions to request.')
            return {}, total
        elif len(overworkers):
            pending.extend(
                await req_overworked_parts(
                    cl, myid, overworkers, num_fair - len(pending)))

    acks = []  # type: List[Partition]
    timeout = 2
    starttime = time()
    # @TODO There should be an asyncio way to do this without using our own
    # timer?
    while len(pending) and (time() - starttime < timeout):
        # Wait for at least one response.
        done, notdone = await asyncio.wait(
            [req.response for req in pending if req.response],
            return_when=asyncio.FIRST_COMPLETED)

        # Check for acks
        acks.extend(await confirm_pending_requests(cl, pending))

    # If timeout passes, cancel remaining requests.
    for req in pending:
        logging.info('%s Removing request from %s',
                     req.requestor, req.recipient)
        schedule(delete_pending(cl, req))
        if req.response:  # type: ignore
            req.response.cancel()  # type: ignore

    if myparts:
        acks.append(myparts.decode())
    await cl.put(path('parts', myid), ','.join(acks))
    # Assigned partitions & last id for those partitions.
    return {p: await get_checkpoint(cl, p) for p in acks}, total


async def register(cl: Client, myid: str, myip: str = '127.0.0.1'):
    lease = await cl.grant_lease(ttl=CHECKIN*10)
    # The stored value is the worker's timeout, after which other
    # workers may consider it dead.
    timeout = time() + CHECKIN*10
    return await cl.put(path('workers', myid, myip), timeout, lease=lease)


async def set_checkpoint(cl: Client, part: str, value: int):
    """
    Set the checkpoint number for a partition.

    >>> from cranial.messaging.test.test_coordinator import _fake_client
    >>> c = _fake_client()
    >>> run(set_checkpoint(c, 'foo', 41))
    True
    >>> run(get_checkpoint(c, 'foo'))
    41
    """
    return await cl.put(path('checkpoint', part), '{},{}'.format(
        value, int(time())))


async def get_all_checkpoints(cl) -> List[kvResult]:
    return await cl.range(span('checkpoint'))


async def get_expired_partitions(cl) -> List[Partition]:
    # @TODO
    return []
    # parts = await get_all_checkpoints(cl)


async def get_checkpoint(cl: Client, part: str) -> Optional[int]:
    """
    Get the checkpoint number for a partition.

    >>> from cranial.messaging.test.test_coordinator import _fake_client
    >>> c = run(_fake_client(PREFIX).preload())
    >>> run(get_checkpoint(c, '1'))
    0
    """
    result = await cl.range(path('checkpoint/' + part))
    if len(result) == 0:
        return None
    return int(result[0][VALUE].decode().split(',')[0])


async def respond(cl: Client, myid: str, myparts: Dict) -> None:
    """
    >>> from cranial.messaging.test.test_coordinator import _fake_client
    >>> c = run(_fake_client(PREFIX).preload())
    >>> assert(run(c.put(path('group-req/4/foo'), '0')))
    >>> assert(run(c.put(path('group-req/5/foo'), '0')))
    >>> assert(run(c.put(path('group-req/6/foo'), '0')))
    >>> run(respond(c, 'b', {'3': None, '4': None}))
    >>> key = path(GROUP_ACK_PATH_3.format('foo', 0, '{}'))
    >>> run(get_single_value(c, key.format(4))) == Response.DENY.value
    True
    >>> run(get_single_value(c, key.format(5))) == Response.DENY.value
    True
    >>> run(get_single_value(c, key.format(6))) == Response.OK.value
    True
    """
    group = await cl.range(span('group-req'))  # type: List[kvResult]
    for r in group:
        part, worker = r[KEY].decode().split('/')[-2:]
        rev = r[VALUE]
        key = path(GROUP_ACK_PATH_3.format(worker, int(rev), part))
        if part not in myparts and await get_single_value(
                cl, path('parts/unassigned', part)) == rev:
            # ACCEPT
            await cl.put(key, Response.OK.value.decode())
        else:
            # DENY
            await cl.put(key, Response.DENY.value.decode())


def run(coroutine):
    loop = asyncio.get_event_loop()
    if LOGLEVEL == 'DEBUG':
        loop.set_debug(True)
    return loop.run_until_complete(coroutine)


def schedule(coroutine) -> asyncio.Future:
    return asyncio.ensure_future(coroutine)


def worker(
        producer: Iterator[Tuple[int, Any]],
        output: Callable,
        cl: Client = None,
        myid: str = None):
    """
    A worker receives a stream of Messages, but only processes Messages having
    an ID that belongs to a partition assigned to the worker.

    Workers collaborate in order to agree which workers are assigned which
    partitions.

    Params

    ------
    producer
    A Generator returning MessageId, Message Pairs

    output
    ------
    A function to which Messages procesed by this worker are passed. Probably a
    Method on a thread-safe object which aggregates Messages.

    Test a single worker here; we test mutliple workers in __main___:
    >>> p = [(4, 4), (5, 5), (6, 6)]
    >>> from cranial.messaging.test.test_coordinator import _fake_client
    >>> c = _fake_client(PREFIX)
    >>> run(init(c))
    >>> result = []
    >>> o = result.append
    >>> worker(p, o, c)
    >>> result
    [4, 5, 6]
    """
    cl = cl or client(endpoint="localhost:2379")
    myid = myid or str(uuid4())
    run(register(cl, myid))
    parts = {}  # type: Dict

    last_checkin = 0  # type: float

    for i, msg in producer:
        if time() - last_checkin > CHECKIN:
            run(respond(cl, myid, parts))
            parts, total_parts = run(req_parts(cl, myid))
            logging.info('Worker %s was assigned %s', myid, parts)
            last_checkin = time()
        for p, last_id in parts.items():
            # @TODO Support non-numeric partition ids.
            if i % total_parts == int(p) and i > (last_id or -1):
                ok = run(set_checkpoint(cl, p, i))
                if ok:
                    parts[p] = i
                    output(msg or i)
                break


def sharing_worker(producer, out, client, myid, how_many):
    worker(producer(how_many, 200),
           out.append,
           client,
           'Worker'+str(myid))


if __name__ == '__main__':
    # Testing!
    # import doctest
    # doctest.testmod()

    from multiprocessing import Process, Manager  # noqa
    from cranial.messaging.test.test_coordinator import (
        _fake_client,
        _serial_producer)
    num_messages = 5

    for num_workers in range(1, 3):  # up to 7 workers.
        manager = Manager()
        out = manager.list()  # type: List[int]
        shared = manager.dict()  # type: Dict[str, str]
        cl = _fake_client(PREFIX)
        cl.store = shared
        run(init(cl))
        logging.info('Starting %s workers.', num_workers)
        ps = [Process(target=sharing_worker,
                      args=(_serial_producer, out, cl, i, num_messages))
              for i in range(num_workers)]
        start = time()
        for p in ps:
            p.start()
        for p in ps:
            p.join()
        logging.info('Duration: %s', time() - start)
        mis = [i for i in range(len(out)) if out[i] != i]
        err = mis if mis == [] else out[mis[0]-5:mis[0]+5]
        if len(out) == num_messages and mis == []:
            result = 'OK! %s messages: %s...'
            report = out[:5]
        else:
            result = 'FAIL :-( %s messages: %s...'
            report = err

        logging.info(result, len(out), report)
