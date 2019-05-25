#!python -o
"""
Cranial Coordinator

Goal:
If you have a reliable producer and a reliable destination, then we provide
at-least-once guaranteed processing for all messages across a moderate number
of unreliable workers with target efficiency equal to Kafka.
Exactly-once processing is possible by check-pointing with the destination.

efficiency = cost per 1000 messages per second
    incl. Workers & at least 3 Brokers.
"""
import asyncio
import logging
from time import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union
from uuid import uuid4

from aioetcd3.client import client, Client
# from toolz import memoize

# type aliases
MsgId = int
Msg = Any

starttime = None
PREFIX = '/cc/'
checkin = 1  # Every n seconds

logging.basicConfig(level='INFO')


def path(key: str):
    return PREFIX + key


def span(key: str):
    """A Range from 'key/' to 'key0'

    >>> span('foo')
    ('/cc/foo/', '/cc/foo0')
    """
    return (path(key + '/'), path(key + "0"))


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


def _producer(duration, mps=1000) -> Iterator[Tuple[MsgId, Msg]]:
    """
    For testing. Produces integers for `duration` seconds, repeating the same
    for up to 1/mps of a second.

    >>> N = 1
    >>> M = 600
    >>> target = int(.9*N*M)
    >>> result = set()
    >>> for i, _ in _producer(N, M):
    ...     result.add(i)
    >>> len(result) > target
    True
    >>> l = list(result)[:target]
    >>> l[:3]
    [0, 1, 2]
    >>> mis = [i for i in range(target) if l[i] != i]
    >>> mis if len(mis) == 0 else l[mis[0]-5:mis[0]+5]
    []
    """
    global starttime
    if not starttime:
        starttime = time()*mps
    i = 0
    while True:
        if i >= duration*mps:
            raise StopIteration
        i = int(time()*mps - starttime)
        yield i, None


class FakeMeta():
    mod_revision = 0

    def __init__(self, rev):
        self.mod_revision = rev


class FakeClient(Client):
    rev = 0
    store = {}  # type: Dict[str, str]
    leases = {}  # type: Dict[float, List[str]]

    def __init__(self, prefix=PREFIX, endpoint=None):
        self.p = prefix

    def preload(self):
        # Initial testing data.
        self.store = {self.p+'parts/total': '6',
                      self.p+'parts/a': '1,2,5,0',
                      self.p+'parts/b': '3,4',
                      self.p+'checkpoint/1': '0'}  # Type: Dict[str, str]
        return self

    async def grant_lease(self, ttl: int):
        t = time() + ttl
        self.leases[t] = []
        return t

    async def range(self, r) -> List[Tuple[bytes, bytes, FakeMeta]]:
        # Delete expired keys.
        for t in self.leases:
            if t < time():
                for key in self.leases[t]:
                    del(self.store[key])
                del(self.leases[t])

        meta = FakeMeta(self.rev)
        self.rev += 1
        if type(r) is tuple:
            r = r[0]
        logging.debug('range %s', r)
        return [(bytes(k, 'utf-8'),
                 bytes(v, 'utf-8'),
                 meta)
                for k, v in self.store.items() if k.startswith(r)]

    async def put(self, key, value, lease=None):
        self.store[str(key)] = str(value)
        if lease:
            self.leases[lease] = str(key)

        # Simulate worker 'a' responding to requests.
        if key == self.p+'req/a':
            logging.debug(value)
            requestor, part, rev = value.split(',')
            if part in self.store[self.p+'parts/a'].split(','):
                self.store['{}checkpoint/{}'.format(self.p, part)] = '42'
                self.store['{}ack/{}/a/{}'.format(
                    self.p, requestor, part)] = rev
            else:
                self.store[self.p+'deny/'+requestor] = 'a,{},{}'.format(
                    part, rev)


def _fake_client(prefix=PREFIX) -> Client:
    return FakeClient(prefix)


async def simple_get(cl: Client, key: str) -> bytes:
    """
    >>> run(simple_get(_fake_client().preload(), 'parts/total'))
    b'6'
    """
    r = await cl.range(path(key))
    logging.debug('Got raw %s: %s', key, r)
    if len(r) < 1:
        return b''
    return b'' if len(r[0]) == 0 else r[0][1]


async def init(cl: Client, num_parts: int = 6):
    """
    >>> cl = FakeClient()
    >>> run(init(cl, 5))
    >>> run(simple_get(cl, 'parts/total'))
    b'5'
    >>> run(simple_get(cl, 'parts/unassigned'))
    b'0,1,2,3,4'
    """
    # @TODO This should be in a transaction?
    await cl.put(path('parts/total'), str(num_parts))
    await cl.put(path('parts/unassigned'), ','.join([str(n) for n in
                                                     list(range(num_parts))]))


async def get_workers(cl: Client):
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


async def req_parts(cl: Client, myid: str) -> Tuple[Dict[str, int], int]:
    """
    >>> run(req_parts(_fake_client().preload(), str(uuid4())))
    ({'1': 42}, 6)
    """
    parts = await cl.range(span('parts'))
    total = None
    logging.debug(parts)
    for p in parts:
        if p[0] == bytes(path('parts/total'), 'utf-8'):
            total = int(p[1].decode())
            break
    if not total:
        raise Exception("Couldn't get total number of partitions")

    overworked = filter(lambda x: b',' in x[1], parts)
    workers = []
    maxparts = 0
    for w in overworked:
        partitions = w[1].decode().split(',')
        num = len(partitions)
        rev = w[2].mod_revision
        workers.append((num, w[0], partitions, rev))
        if num > maxparts:
            maxparts = num

    # No available partitions. @TODO Ask for partition increase.
    if maxparts <= 1:
        return {}, total

    # Sorted list
    pending = []
    count = 0
    for p in sorted(workers, key=lambda x: x[0]):
        logging.debug('Worker: %s', p)
        name = p[1].decode().split('/')[-1]
        partitions = p[2]
        rev = p[3]
        if count < maxparts - 1:
            key = path('req/{}'.format(name))
            value = '{},{},{}'.format(myid, partitions[0], rev)
            await cl.put(key, value)
            logging.debug('Putting %s: %s', key, value)
            logging.debug(cl.store)
            count += 1
            pending.append(['ack/{}/{}/{}'.format(myid, name, partitions[0]),
                            rev,
                            partitions[0]])
            logging.debug('Pending: %s', pending)
        else:
            break

    starttime = time()
    acks = []
    timeout = 5
    while len(pending) and time() - starttime < timeout:
        # Check for acks
        key, value, part = pending.pop(0)
        confirm = await simple_get(cl, key)
        logging.debug('Got %s: %s =?= %s', key, confirm, value)
        logging.debug(cl.store)
        if confirm == str(value).encode():
            acks.append(part)

    # Assigned partitions & last id for those partitions.
    [await cl.put(path('parts/'+myid), p) for p in acks]
    return {p: int((await simple_get(cl, 'checkpoint/'+p)).decode())
            for p in acks}, total


async def register(cl: Client, myid: str, myip: str = '127.0.0.1'):
    # @TODO use lease to expire
    lease = await cl.grant_lease(ttl=checkin*10)
    return await cl.put(path('workers/'+myid), myip, lease=lease)


async def set_checkpoint(cl: Client, part: str, value: int):
    """
    Set the checkpoint number for a partition.

    >>> c = _fake_client()
    >>> run(set_checkpoint(c, 'foo', 41))
    >>> run(get_checkpoint(c, 'foo'))
    41
    """
    return await cl.put(path('checkpoint/' + part),  str(value))


async def get_checkpoint(cl: Client, part: str) -> Optional[int]:
    """
    Get the checkpoint number for a partition.

    >>> c = _fake_client().preload()
    >>> run(get_checkpoint(c, '1'))
    0
    """
    result = await cl.range(path('checkpoint/' + part))
    if len(result) == 0:
        return None
    return int(result[0][1].decode())


def run(coroutine):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coroutine)


def worker(
        producer: Iterator[Tuple[int, Any]],
        output: Callable,
        cl: Client = None):
    """
    A worker receives a stream of Messages, but only processes Messages having
    an ID that belongs to a partition assigned to the worker.

    Workers collaborate in order to agree which workers are assigned which
    partitions.

    Params

    ------
    producer
    A function returning MessageId, Message Pairs

    output
    ------
    A function to which Messages procesed by this worker are passed. Probably a
    Method on a thread-safe object which aggregates Messages.

    Test a single worker here; we test mutliple workers in __main___:
    >>> p = [(4, 4), (5, 5), (6, 6)]
    >>> c = FakeClient()
    >>> run(init(c))
    >>> result = []
    >>> o = result.append
    >>> worker(p, o, c)
    >>> result
    [4, 5, 6]
    """
    cl = cl or client(endpoint="localhost:2379")
    myid = str(uuid4())
    run(register(cl, myid))

    last_checkin = 0

    for i, msg in producer:
        if time() - last_checkin > checkin:
            parts, total_parts = run(req_parts(cl, myid))
            logging.info('Worker %s was assigned %s', myid, parts)
        for p, last_id in parts.items():
            # @TODO Support non-numeric partition ids.
            if i % total_parts == int(p) and i > last_id:
                ok = run(set_checkpoint(cl, p, i))
                if ok:
                    parts[p] = i
                    output(msg)
                break


if __name__ == '__main__':
    # Testing!
    import doctest
    doctest.testmod()

    from multiprocessing import Process, Manager  # noqa

    for num_workers in range(1, 8):  # up to 7 workers.
        manager = Manager()
        shared = manager.Namespace()
        shared.client = _fake_client()  # type: ignore
        shared.out = []  # type: ignore
        logging.info('Starting %s workers.', num_workers)
        ps = [Process(target=worker,
                      args=(_producer(1),
                            shared.out.append,  # type: ignore
                            shared.client))  # type: ignore
              for _ in range(num_workers)]
        start = time()
        for p in ps:
            p.start()
        for p in ps:
            p.join()
        logging.info('Duration: %s', time() - start)
        success = shared.out[:500] == list(range(500))  # type: ignore
        logging.info(
                'OK! %s messages %s' if success else 'FAIL :-( %s %s...',
                len(shared.out), shared.out[:5])  # type: ignore
