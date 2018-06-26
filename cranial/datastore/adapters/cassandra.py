from cassandra.encoder import Encoder
from cranial.common import logger

log = logger.get()


class Adapter():
    def __init__(self, session):
        self.session = session
        self.res = None
        # Use Cassandra Encoder to prevent SQL Injection
        self.encoder = Encoder()

    def sanitize(self, q, params):
        """
        Basic prevention against SQL Injection.
        >>> adapter = CassandraAdapter(None)
        >>> adapter.sanitize('SQL {}', ["injection'; DROP DATABASE;--"])
        "SQL 'injection''; DROP DATABASE;--'"
        """
        sanitized = map(self.encoder.cql_encode_all_types, params)
        return q.replace('?', '{}').format(*list(sanitized))

    def execute(self, q, params=None, executor=None):
        if executor is None:
            executor = self.session.execute

        try:
            if type(q) is str and params is not None and len(params):
                q = self.sanitize(q, params)
                self.res = executor(q)
            else:
                self.res = executor(q, params)
        except Exception as e:
            log.error('Exception {} on {}; {}'.format(e, q, str(params)))
            raise e

        self.rowcount = len(self.res.current_rows) if hasattr(self.res, 'current_rows') else None
        columns = getattr(self.res, 'column_names', [])
        self.description = [[c] for c in columns] if columns is not None else []
        return self.res

    def execute_async(self, q, params=None):
        return self.execute(q, params, executor=self.session.execute_async)

    def get_async(self):
        # Block for Async results...
        if hasattr(self.res, 'result'):
            self.res = self.res.result()
            self.rowcount = len(self.res.current_rows)
            columns = getattr(self.res, 'column_names', [])
            self.description = [[c] for c in columns] if columns is not None else []


    def fetchone(self):
        self.get_async()
        if self.rowcount:
            return self.res[0]
        else:
            # @ToDo This seems to assume KeyValueDB. We should generalize.
            raise KeyError('Key not found.')

    def fetchall(self):
        self.get_async()
        return self.res

    def __getattr__(self, name):
        """ Proxy anything else to the session object, for cases where DBAPI2
        compatible things already exist."""
        return getattr(self.session, name)


# Alias
CassandraAdapter = Adapter
