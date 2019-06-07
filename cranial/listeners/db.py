from collections import deque
from time import sleep
from typing import Any, Deque  # noqa

from cranial.common import logger
from cranial.datastore.dbapi import Param, render_params
from cranial.listeners import base

log = logger.create('db_listener',
                    logger.fallback('LISTENERS_LOGLEVEL', 'WARNING'))


class Listener(base.Listener):
    def __init__(self,
                 cursor,
                 table: str,
                 id: str = 'id',
                 last_id: Any = None,
                 limit=1000,
                 sleep=10,
                 select='*',
                 where=None,
                 **kwargs) -> None:
        """
        Parameters
        ----------
        cursor
        A DBAPI2 cursor.

        table
        A table to poll for new rows.

        id
        An incrementing column for identifying new rows.

        last_id
        This will iterate rows having id > to this value. If not provided,
        defaults to the tables MAX(id) at the time of init. If you want to
        deliever all rows currently in the table, set to `0` or an effective
        gobal minimum for the datatype.

        limit
        Maximum number of rows to retrieve in a single query.
        """
        self.cursor = cursor
        # split allows to support aliases eg, "id as table_id"
        id_parts = id.split(' ')
        self.id_col = id_parts[0]
        self.id_alias = id_parts[-1]
        self.query_head = 'SELECT {} FROM {} WHERE {} > '.format(
            select, table, self.id_col)
        if not last_id:
            cursor.execute("SELECT MAX({}) FROM {} WHERE 1=1 ".format(
                           self.id_col, table))
            last_id = cursor.fetchone()[0]
        self.last_id = int(last_id)
        self.limit = limit
        self.queue = deque()  # type: Deque
        self.col_names = None
        self.sleeptime = int(sleep)
        self.where = where

    def _queue_results(self):
        # render_params helps because we don't want to assume the data type of
        # the id column.
        where = '' if not self.where else 'AND {}'.format(self.where)
        # If a user wnats to provide a where clause, they are responsible for
        # getting the syntax right for their DB. @TODO we could make this
        # a list of (Column, Operator, Value) tuples and use Param on the
        # Values.
        chunks = (self.query_head,
                  Param(self.last_id),
                  where,
                  ' ORDER BY {} ASC LIMIT {}'.format(self.id_col, self.limit))
        query, params = render_params(self.cursor, chunks)
        self.cursor.execute(query, params)
        if not self.col_names:
            self.col_names = [x[0] for x in self.cursor.description]
        for row in self.cursor.fetchall():
            record = dict(zip(self.col_names, row))
            if record[self.id_alias] > self.last_id:
                self.last_id = record[self.id_alias]
            self.queue.append(record)

    def recv(self, **kwargs):
        while len(self.queue) == 0:
            self._queue_results()
            if len(self.queue) == 0:
                sleep(self.sleeptime)
        return self.queue.popleft()
