from cranial.datastore.adapters import psql
from cranial.listeners import db


class Listener(db.Listener):
    """Provides a connection to a PostgreSQL databse using credentials stored
    in ~/.pgpass.
    """
    def __init__(self,
                 address,
                 table,
                 dbname: str = None,
                 endpoint: str = None,  # To support URI factory.
                 id='id',
                 last_id=None,
                 limit=1000,
                 **kwargs):
        host, port = address.split(':')
        dbname = dbname or endpoint
        cur = psql.get_cursor(dbname=dbname,
                              host=host,
                              port=port,
                              user=kwargs.get('user'))
        super().__init__(cur, table, id, last_id, limit, **kwargs)
