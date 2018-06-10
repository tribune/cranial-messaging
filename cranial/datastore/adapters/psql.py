'''
Helper functions to work with postgres/redshift.
'''
from select import select
from typing import List

import psycopg2
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE

from cranial import logger

log = logger.get()

default_config = {
    'keepalives': '1',
    'keepalives_idle': '6',
    'keepalives_interval': '20',
    'keepalives_count': '1'}

def wait_select_inter(conn):
    """
    cancel query by crtl-c
    http://initd.org/psycopg/articles/2014/07/20/cancelling-postgresql-statements-python/
    """
    while 1:
        try:
            state = conn.poll()
            if state == POLL_OK:
                break
            elif state == POLL_READ:
                select([conn.fileno()], [], [])
            elif state == POLL_WRITE:
                select([], [conn.fileno()], [])
            else:
                raise conn.OperationalError(
                    "bad state from poll: %s" % state)
        except KeyboardInterrupt:
            conn.cancel()
            # the loop will be broken by a server error
            continue


psycopg2.extensions.set_wait_callback(wait_select_inter)

credentials = None  # type: List

def get_credentials(pgpass='~/.pgpass', append=False):
    global credentials
    if credentials is None or append:
        credentials = credentials or []

        with open(pgpass) as f:
            for line in f.readlines():
                line = line.strip('\n')
                parts = line.split(":")
                c = default_config.copy()
                c['host'], c['port'], c['dbname'], c['user'], c['password'] = parts

                # build connection string
                conn_str = ' '.join(["{}='{}'".format(k, v)
                                    for k, v in c.items()])
                credentials.append(conn_str)

    return credentials


def get_cursor(host: str = None, user: str = None, credentials_file='~/.pgpass'):

    conn_str = next(filter(lambda x: x['host'] == host and x['user'] == user,
                      get_credentials(credentials_file)))

    if not conn_str:
        raise Exception('No such credentials available.')

    # connect
    conn = psycopg2.connect(conn_str)
    conn.autocommit = True
    return conn.cursor()


def query(q: str, credentials_file='.pgpass'):
    '''
    Execute a query in redshift

    Parameters
    ----------
    q : str
        psql query string


    Returns
    -------
        query results

    '''
    with get_cursor(credentials_file=credentials_file) as cur:
        # execute
        cur.execute(q)

        # fetch results
        try:
            res = cur.fetchall()
            # get result column names
            cols = [c.name for c in cur.description]
        except Exception as e:
            log.warn("Could not fetch results for query: " + q)
            raise e

    return (res, cols)
