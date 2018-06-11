from multiprocessing import Process, Queue
from os import environ
from queue import Empty
from time import sleep

from cranial.messaging.adapters import firehose

queue = Queue()  # type: Queue
process = None


def worker(q: Queue):
    while True:
        try:
            args = q.get_nowait()
            firehose.put_data(*args)
        except Empty:
            sleep(float(environ.get('FIREHOUSE_ASYNC_SLEEP', .1)))


def put_data(*args):
    global queue
    queue.put_nowait(args)

    global process
    if process is None:
        process = Process(target=worker, args=(queue,))
        process.start()
