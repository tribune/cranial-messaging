'''
Helpers for postgres/redshift.
'''
import os
from select import select
from typing import Dict, List, Optional  # noqa

import psycopg2
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE

from cranial.common import logger

log = logger.get()

default_config = {
    'keepalives': '1',
    'keepalives_idle': '6',
    'keepalives_interval': '20',
    'keepalives_count': '1'}  # type: Dict[str, Optional[str]]


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

credentials = None  # type: Optional[List]


def _split_parts(parts: List[str]) -> Dict[str, Optional[str]]:
    c = default_config.copy()
    if len(parts) < 5:
        return c
    c['host'], c['port'], c['dbname'], c['user'], c['password'] = parts
    for key, value in c.items():
        if value == '*':
            c[key] = None
        else:
            c[key] = value
    return c


def get_credentials(pgpass='~/.pgpass', append=False):
    global credentials
    if credentials is None or append:
        credentials = credentials or []

        with open(os.path.expanduser(pgpass)) as f:
            for line in f.readlines():
                line = line.strip('\n')
                parts = line.split(":")
                connect_params = _split_parts(parts)
                credentials.append(connect_params)

    return credentials


def find_credentials(credentials_file='~/.pgpass', **kwargs) -> Dict[str, str]:
    """
    Returns the first credentials from pgpass that contain all the values
    passed in.

    Supported Parameters:
      host, port, dbname, user, password

    >>> from tempfile import mkstemp
    >>> _, path = mkstemp()
    >>> f = open(path, 'w')
    >>> _ = f.write("myhost:myport:mydbname:myname:mypass\\n")
    >>> _ = f.write("byhost:byport:bydbname:byname:bypass")
    >>> f.close()
    >>> assert(find_credentials(path, user='myname')['password'] == 'mypass')
    >>> assert(find_credentials(path, dbname='bydbname')['user'] == 'byname')
    >>> assert(find_credentials(path, host='not-host'))
    Traceback (most recent call last):
    ...
    Exception: No such credentials available.
    """
    creds = get_credentials(credentials_file)
    for key, value in kwargs.items():
        if value:
            creds = list(filter(lambda x: x.get(key) == value, creds))

    if len(creds) == 0:
        raise Exception('No such credentials available.')

    return creds[0]


def get_cursor(credentials_file='~/.pgpass', **kwargs):
    c = find_credentials(credentials_file, **kwargs)
    return SingleCursorDatabaseConnector(c['dbname'], c['host'], c['port'],
                                         c['user'], c['password'])


def query(q: str, credentials_file='.pgpass'):
    '''
    Execute a query in redshift/postgres.

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


class SingleCursorDatabaseConnector(object):
    """
    Wraps the psycopg2 connection and cursor functions to reconnect on close.

    SingleCursorDatabaseConnector maintains a single cursor to a psycopg2
    database connection. Lazy initialization is used, so all connection and
    cursor management happens on the method calls.
    """

    def __init__(self,
                 database,
                 host='localhost',
                 port=5439,
                 user='postgres',
                 password='postgres',
                 autocommit=True):
        """
        Lazy constructor for the class.

        Parameters
        ----------
        database : str
            The name of the database to connect to
        host : str
            Host name of the server the database is running on
        port : int
            Port the database is running on
        user : str
            Username to connect as
        password : str
            Password for the connection
        autocommit : bool
            If True, then no transaction is left open. All commands have
            immediate effect.
        """
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.autocommit = autocommit

        self._cursor = None
        self._connection = None

    def _connect(self):
        self._connection = psycopg2.connect(database=self.database,
                                            host=self.host,
                                            port=self.port,
                                            user=self.user,
                                            password=self.password)
        self._connection.autocommit = self.autocommit

        return self._connection

    def _get_connection(self):
        if self._connection and not self._connection.closed:
            return self._connection

        self._cursor = None
        return self._connect()

    def _get_cursor(self):
        if self._cursor and not self._cursor.closed:
            return self._cursor

        self._cursor = self._get_connection().cursor()

        return self._cursor

    def execute(self, *args, **kwargs):
        return self._get_cursor().execute(*args, **kwargs)

    def fetchone(self):
        return self._get_cursor().fetchone()

    def fetchmany(self, *args, **kwargs):
        return self._get_cursor().fetchmany(*args, **kwargs)

    def fetchall(self):
        return self._get_cursor().fetchall()

    def __getattr__(self, name):
        return getattr(self._get_cursor(), name)

    def __iter__(self):
        return self._get_cursor()

    @property
    def statusmessage(self):
        cursor = self._get_cursor()

        if cursor:
            return cursor.statusmessage
        else:
            return None

    def commit(self):
        self._get_connection().commit()

    def rollback(self):
        self._get_connection().rollback()

    def close(self):
        self._get_connection().close()
