import pickle
import json
from typing import Iterable
from cachetools import TTLCache

from cranial.common import logger
from cranial.datastore.dbapi import get_temp_db

log = logger.get(name='cranial.keyvalue')

# Default Databse Column type for values when blob is False
DEFAULT_TYPE = 'text'


class KeyValueDB(object):
    '''This object should mostly work like a dictionary, except it reads and
    writes from an external database. Subclasses can implement caching as
    appropriate.

    This class assumes that the database enforces uniqueness of the keys. I.e.,
    all SELECT queries will have LIMIT 1.

    >>> kv = get_mock()
    >>> kv['foo'] = 'a'
    >>> kv[1] = 'int'
    >>> kv.cache_clear()
    >>> kv['foo']
    'a'
    >>> kv[1]
    'int'
    >>> kv['bar'] = 'b'
    >>> kv.cache_clear()
    >>> kv['bar']
    'b'
    >>> kv['foo'] = 'c'
    >>> kv.cache_clear()
    >>> kv['foo'] = 'd'
    >>> kv.cache_clear()
    >>> kv['foo']
    'd'
    >>> kv['bar']
    'b'
    >>> kv.db.connection.close()
    '''

    def __init__(self, cursor,
                 table: str,
                 create_table: bool = False,
                 truncate_table: bool = False,
                 drop_table: bool = False,
                 keycol: str = 'key',
                 valcol: str = 'value',
                 blob=True,
                 keytype: str = 'text',
                 valtype: str = DEFAULT_TYPE,  # Default if `blob` is False.
                 placeholder: str = '?',
                 commit_fn: callable = None,
                 cachesize=2 ** 10,
                 cacheduration=3600):
        """Create a KeyValueDB object with  given connection and table.

        By default, the value type is pickled blob. Users can pass `blob=False`
        to serialize data as JSON in a text field, or `valtype='TYPE'` where
        TYPE is any type supported by the database.

        Parameters
        ----------
        cursor
        A DBAPI2 cursor, or at least an object with an .execute()
        method that accepts SQL query and a .fetchone() method that
        returns a row.

        table
        A table with 'key' and 'value' columns in the connected DB.

        create_table
        If True, execute 'create table if not exists ...'

        keycol
        Column name for key values, default 'key'

        valcol
        Column name for value values, default 'value'

        keytype
        If create_table, the datatype for the key. Default 'text'.

        valtype
        If create_table, the datatype for the value. Default 'blob' when
        `blob` parameter (below) is true, otherwise, 'text.'

        blob **DEPRECATED**
        If True, use pickle to (de)serialize, if False, use json. To support
        this, the cursor.execute() method must be able to accept a `bytes`
        parameter.

        placeholder
        The placeholder character required by the cursor's `paramstyle`.
        Defaults to '?'.

        commit_fn
        An optional function that will be called after writes with
        no arguments. If using a transactional DBAPI2 cursor without
        autocommit, this should probably be cursor.connection.commit
        unless you are handling transactions elsewhere.


        cachesize
        Max size for the LRU cache.

        cacheduration
        Seconds after which cache items will be expired.

        """
        self.db = cursor
        self.table = table
        self.keycol = keycol
        self.keytype = keytype
        self.valcol = valcol
        self.valtype = 'blob' if blob and valtype is DEFAULT_TYPE else valtype
        self.placeholder = placeholder
        self.commit = commit_fn
        self.cache = TTLCache(maxsize=cachesize, ttl=cacheduration)
        if create_table:
            self._create_table(drop_table)
        elif truncate_table:
            self.db.execute("TRUNCATE {};".format(table))

        self._get_table_columns()

    def _get_table_columns(self):
        """
        Not ideal way to look up what columns are there in the underlying table
        # @TODO: make this less dependent on database type
        """
        try:
            # Better for Cassandra/Scylla.
            self.db.execute("SELECT column_name FROM system_schema.columns "
                            "WHERE keyspace_name = 'default' and table_name = '{}'".format(self.table))
            self.table_columns = [r.column_name for r in self.db.fetchall()]
        except:
            log.info("Couldn't get columns through system_schema.")

            try:
                # For DBAPI2 databases.
                self.db.execute("SELECT * FROM {} LIMIT 1".format(self.table))
                self.table_columns = [d[0] for d in self.db.description]
            except:
                log.warning("Couldn't get columns through row.")

        if not self.table_columns:
            raise Exception("Could not initialize table_columns. Maybe table does not exist.")
        else:
            log.info("{} table has these columns: {}".format(self.table, self.table_columns))

        return self.table_columns

    def _create_table(self, drop_table):
        """
        helper method to run 'create table' query,
        optionally drop the table if it exists before creating it again
        Parameters
        ----------
        drop_table
            if True, try dropping table before creating
        """
        if drop_table:
            log.warning("Dropping table if exists: {}".format(self.table))
            self.db.execute(
                "DROP TABLE IF EXISTS {t} ".format(t=self.table))
            self.db.execute(
                """CREATE TABLE IF NOT EXISTS {t} (
                {k} {ktype} PRIMARY KEY,
                {v} {vtype});""".format(t=self.table,
                                        k=self.keycol,
                                        ktype=self.keytype,
                                        v=self.valcol,
                                        vtype=self.valtype))

    def _to_blob(self, v):
        return pickle.dumps(v, protocol=4) \
            if self.valtype is 'blob' else json.dumps(v)

    def __getitem__(self, key):
        """
        Basic get/set is tested throughout the class. Here we test the cache.

        >>> kv = get_mock()
        >>> kv['witch'] = 'duck'
        >>> 'witch' in kv.cache
        True
        """
        if not (key in self.cache):
            base_query = "SELECT {} from {} where {} = {} LIMIT 1".format(
                self.valcol, self.table, self.keycol, self.placeholder)
            self.db.execute(base_query, [key])
            result = self.db.fetchone()[0]
            unserialized = pickle.loads(result) \
                if self.valtype is 'blob' else json.loads(result)
            self.cache[key] = unserialized

        return self.cache[key]

    def get(self, key, default=None):
        """
        analog of dict.get

        Parameters
        ----------
        key
            key

        default
            if key is not found, return this value, default is None

        Returns
        -------
            value corresponding to the key, or default
        """
        try:
            result = self[key]
            if result is None:
                return default
        except KeyError as e:
            log.debug(str(e))
            return default
        except Exception as e:
            log.warning(str(e))
            return default

        return result

    def __setitem__(self, key, value):
        serialized_value = self._to_blob(value)
        # Subjectively, UPDATE||INSERT is faster than DEL+INSERT in Redshift.
        # This could possibly be optimized for Cassandra, it works fine.
        try:
            base_query = "UPDATE {} SET {} = {} WHERE {} = {}".format(
                self.table, self.valcol, self.placeholder, self.keycol,
                self.placeholder)
            self.db.execute(base_query, [serialized_value, key])
            if key in self.cache:
                del (self.cache[key])

            if self.db.rowcount < 1:
                base_query = "INSERT INTO {} ({}, {}) VALUES ({}, {})".format(
                    self.table, self.keycol, self.valcol,
                    self.placeholder, self.placeholder)
                self.db.execute(base_query, [key, serialized_value])
                self.cache[key] = value
            if self.commit:
                self.commit()
            return value
        except Exception as e:
            log.error('{}: {}'.format(type(e), e))
            log.error("Could not update {} => {}".format(key, value))

    def __delitem__(self, key):
        base_query = "DELETE from {} where {} = {}".format(
            self.table, self.keycol, self.placeholder)
        self.db.execute(base_query, [key])
        if key in self.cache:
            del (self.cache[key])
        if self.commit:
            self.commit()
        return key

    def __missing__(self, key):
        raise KeyError('No such key.')

    def cache_clear(self):
        """
        >>> kv = get_mock()
        >>> kv['witch'] = 'duck'
        >>> kv.cache_clear()
        >>> 'witch' in kv.cache
        False
        """
        self.cache = TTLCache(self.cache.maxsize, self.cache.ttl)

    def keys(self):
        self.db.execute("SELECT distinct {} from {}".format(
            self.keycol, self.table))
        for r in self.db.fetchall():
            yield r[0]

    def __len__(self):
        return self.db.execute("SELECT COUNT(1) from {}".format(
            self.table)).fetchone()[0]


class KeyValueReadOnly(KeyValueDB):
    """Access a Key-Value table with protection against writes.

    >>> kv = get_mock()
    >>> kv['foo'] = 'a'
    >>> kv[1] = 'int'
    >>> kv.cache_clear()
    >>> kv['foo']
    'a'
    >>> ro = KeyValueReadOnly(kv.db, kv.table, blob=False)
    >>> ro['foo']
    'a'
    >>> ro['foo'] = 'b'
    Traceback (most recent call last):
      File "keyvalue.py", line 000, in __setitem__
        raise Exception('Read-only data.')
    Exception: Read-only data.
    >>> del(ro['foo'])
    Traceback (most recent call last):
      File "keyvalue.py", line 000, in __delitem__
        raise Exception('Read-only data.')
    Exception: Read-only data.
    """

    def __init__(self, cursor, table: str,
                 keycol: str = 'key', valcol: str = 'value', blob=True,
                 keytype: str = 'text', valtype: str = DEFAULT_TYPE,
                 placeholder: str = '?'):
        super().__init__(cursor=cursor,
                         table=table,
                         keycol=keycol,
                         valcol=valcol,
                         blob=blob,
                         keytype=keytype,
                         valtype=valtype,
                         placeholder=placeholder)

    def __setitem__(self, key, value):
        raise Exception('Read-only data.')

    def __delitem__(self, key):
        raise Exception('Read-only data.')


""" Module Functions. """


class StringSerDe():
    def dumps(self, v):
        return str(v)

    def loads(self, v):
        # Return None as-is to indicate a Null column.
        return str(v) if v is not None else v


class BoolIntSerDe():
    def dumps(self, v):
        if v is True:
            return 1
        elif v is False:
            return 0
        else:
            return v

    def loads(self, v):
        if v is 1:
            return True
        elif v is 0:
            return False
        else:
            return v


class NoOpSerDe():
    def dumps(self, v):
        return v

    def loads(self, v):
        return v


class KeyRowDB(KeyValueDB):
    """ This should work well for Cassandra/Scylla, and OK on traditional
    Relational DBs with an index on the key. You Probably don't want to use this
    on Redshift, though performance should be acceptable if the key is your
    table's SORTKEY.

    Tests to demostrate interoperability with KeyValueDB class:

    >>> kv = get_mock()
    >>> kr = KeyRowDB(kv.db, kv.table)
    >>> kv['foo'] = 'a'
    >>> kr['foo']['value'] == 'a' and kr['foo']['other'] == None
    True
    >>> kr['foo'] = {'other': 'x', 'value': 'b'}
    >>> kv.cache_clear()
    >>> kv['foo']
    'b'
    >>> kr['foo'] = {'key': 'foo', 'other': 'x', 'value': 'b'}
    >>> kv['foo'] = 'c'
    >>> kr.cache_clear()
    >>> kr['foo']['value'] == 'c' and kr['foo']['other'] == 'x'
    True
    """

    def __init__(self, cursor,
                 table: str,
                 valcols: Iterable = None,
                 create_table: bool = False,
                 truncate_table: bool = False,
                 drop_table: bool = False,
                 keycol: str = 'key',
                 keytype: str = DEFAULT_TYPE,
                 valtype: str = DEFAULT_TYPE,
                 placeholder: str = '?',
                 commit_fn: callable = None,
                 blob=False,
                 serde: 'loads/dumps' = json,
                 cachesize=2 ** 10,
                 cacheduration=3600):
        """
        If table creation is desired, the user must provide valcols. By default,
        values will be stored in text columns as json. Alternatively, the user
        may set either `blob = True` to store pickled bytes, or set `serde` to
        an object that will convert values to/from strings.


        >>> cur = get_temp_db()
        >>> cur.execute("CREATE TABLE simple (key VARCHAR, knights VARCHAR)"
        ...   ).connection.commit()
        >>> simple = KeyRowDB(cur, 'simple')
        >>> simple['spam'] = {'knights': 'ni!'}
        >>> cur.execute("SELECT * FROM simple").fetchone()
        ('spam', '"ni!"')
        >>> dropped = KeyRowDB(cur, 'simple', valcols=['different'],
        ...  drop_table=True, create_table=True)
        >>> cur.execute("SELECT * FROM simple").fetchone() is None
        True
        >>> created = KeyRowDB(cur, 'created', valcols=['spam'],
        ...   create_table=True)
        >>> created['camelot'] = {'spam': 'alot'}
        >>> pickled = KeyRowDB(cur, 'pickled', valcols=['v'],
        ...  create_table=True, blob=True)
        >>> pickled['grenade'] = {'v': {'count': 5}}
        >>> pickled.cache_clear()
        >>> pickled['grenade']['v']['count'] is 5
        True
        >>> numeric = KeyRowDB(cur, 'numeric', keycol='num', keytype='int',
        ...   valcols= ['word'], create_table=True)
        >>> numeric[1] = {'word': 'foo'}
        >>> cur.execute("SELECT * FROM numeric").fetchone()
        (1, '"foo"')
        """
        assert callable(serde.loads)
        assert callable(serde.dumps)

        self.valcols = valcols
        super().__init__(cursor=cursor,
                         table=table,
                         keycol=keycol,
                         keytype=keytype,
                         valtype=valtype,
                         placeholder=placeholder,
                         blob=blob,
                         create_table=create_table,
                         truncate_table=truncate_table,
                         drop_table=drop_table,
                         commit_fn=commit_fn,
                         cachesize=cachesize,
                         cacheduration=cacheduration)

        del self.valcol
        if not create_table:
            existing_cols = set(self.table_columns)
            if self.valcols is not None:
                req_cols = set(self.valcols)
                assert all([c in existing_cols for c in req_cols]), \
                    "these columns do not exist in the already existing table: {}".format(req_cols - existing_cols)

        self.serde = pickle if self.valtype is 'blob' else serde

    def _check_coltypes(self, cols, coltype):
        """
        make sure that coltype is a dict where keys are column names and values are their data types

        Parameters
        ----------
        cols
            list of column names

        coltype
            - if dict, make sure all columns are in dict keys and fill with default data type if missing
            - if iterable, has to have the same number of items as column names,
                the two lists will be zipped together
            - if str, this type will be used for all columns

        Returns
        -------
            dict with {column:datatype}
        """
        if isinstance(coltype, dict):
            coltype = {c: coltype.get(c, DEFAULT_TYPE) for c in cols}
        elif isinstance(coltype, (list, tuple)):
            assert len(cols) == len(coltype), "when column types are given as list the " \
                                              "number should match the number of columns"
            coltype = dict(zip(cols, coltype))
        elif isinstance(coltype, str):
            coltype = {c: coltype for c in cols}
        else:
            raise Exception("column types should be either dict, list, tuple or string")
        return coltype

    def _create_table(self, drop_table):
        """
        >>> cur = get_temp_db()
        >>> serde = StringSerDe()
        >>> kr = KeyRowDB(cur, 'newtable', commit_fn=cur.connection.commit,
        ...               create_table = True,
        ...               valcols=['foo', 'bar' , 'baz'], serde=serde)
        >>> kr['spam'] = {'foo': 'a', 'bar': 'b'}
        >>> kr.cache_clear()
        >>> kr['spam'] == {'foo': 'a', 'bar': 'b', 'baz': None}
        True
        """
        if not self.valcols:
            raise Exception("Can't create table without defined columns.")
        if drop_table:
            log.warning("Dropping table if exists {}".format(self.table))
            self.db.execute(
                "DROP TABLE IF EXISTS {t} ".format(t=self.table))

        query = self._create_table_query()
        self.db.execute(query)
        log.info("Created table {} (if it didn't exist already)".format(
            self.table))

    def _create_table_query(self):
        """
        Factor out query definition and its execution from _create_table method
        """
        # make sure column types are set for every column
        self.valtype = self._check_coltypes(self.valcols, self.valtype)

        # Initialize columns with Key. Then append Additional columns.
        columns = ['{} {} PRIMARY KEY'.format(self.keycol, self.keytype)]
        columns += ['{} {}'.format(name, dtype)
                    for name, dtype in self.valtype.items()]
        query = "CREATE TABLE IF NOT EXISTS {table} ({columns});".format(
            table=self.table, columns=','.join(columns))
        return query

    def _execute_get_query(self, key):
        """
        Factor out the query definition and its execution from __getitem__.
        """
        # @TODO Update to support varying Column Types.
        base_query = "SELECT {} from {} where {} = {} LIMIT 1".format(
            '*' if not self.valcols else ','.join(self.valcols),
            self.table,
            self.keycol,
            self.placeholder)
        log.debug(base_query)
        self.db.execute(base_query, [key])

    def get(self, key, default=None):
        """

        Parameters
        ----------
        key
        default
            1. Can be a dict, with valcols as keys and values as default values
               for each column.
            2. Can be a list with the same number of values as the number of
               valcols, list items correspond
               to individual default values for each of the valcols.
            3. Can be anything else, in this case the same default value will
               be used for all columns.
            If there is a need to use default value of dict or list type for
            all columns - replicate the value and use option 1 or 2.

        Returns
        -------

        """
        if default is not None:
            for k in default:
                assert k in (self.valcols or self.table_columns)
        return super().get(key, default)

    def __getitem__(self, key):
        """

        Basic get/set is tested throughout the class. Here we test the cache.

        >>> cur = get_temp_db()
        >>> kr = KeyRowDB(cur, 'simple', valcols=['weight'], create_table=True)
        >>> kr['witch'] = {'weight': 'duck'}
        >>> 'witch' in kr.cache
        True
        """
        if not (key in self.cache):
            self._execute_get_query(key)
            columns = self.valcols or self.table_columns
            result = self.db.fetchone()
            if not result:
                raise KeyError("key {} is not found".format(key))
            # Pair values with column name.
            values_zip = zip(columns, result)

            # Deserialize.
            values = {}
            for k, v in values_zip:
                try:
                    v = self.serde.loads(v)
                except Exception as e:
                    # The Key column usually isn't serialized.
                    if v is not None and k != self.keycol:
                        log.warning('Exception for key {}: {}'.format(k, v))
                        log.warning(e)
                # If Deserializing failed, fall-through to using the column
                # data as-is.
                values[k] = v

            self.cache[key] = values

        return self.cache[key]

    def __setitem__(self, key, values: dict):
        assert type(values) is dict

        # make sure there is no keycol in values dict
        values.pop(self.keycol, None)
        data = [[f, self.serde.dumps(v)] for f, v in values.items()]

        try:
            values_str = ', '.join(
                ['{} = {}'.format(f, self.placeholder) for f, v in data])
            base_query = "UPDATE {} SET {}".format(self.table, values_str)
            base_query += " WHERE {} = {}".format(self.keycol, self.placeholder)
            log.debug(base_query)
            self.db.execute(base_query, [v for f, v in data] + [key])

            if self.db.rowcount < 1:
                data.append([self.keycol, key])
                base_query = "INSERT INTO {} ({}) VALUES ({})".format(
                    self.table,
                    ', '.join([f for f, v in data]),
                    ', '.join([self.placeholder for _ in data]))
                log.debug(base_query)
                self.db.execute(base_query, [v for f, v in data])

            self.cache[key] = values

            if self.commit:
                self.commit()
            return values
        except Exception as e:
            log.error('{}: {}'.format(type(e), e))
            log.error("Could not update {} => {}".format(key, values))


class CompoundKeyRowDB(KeyRowDB):
    """ This CompoundKeyRowDB object takes tuples as keys.
    @TODO It might be an improvement to eliminate this class and simply have
    the code here used in KeyRowDB when KeyRowDB.keycol is a tuple.
    """

    def __init__(self, cursor,
                 table: str,
                 keycols: Iterable = None,
                 valcols: Iterable = None,
                 create_table: bool = False,
                 truncate_table: bool = False,
                 drop_table: bool = False,
                 keytype = DEFAULT_TYPE,
                 valtype = DEFAULT_TYPE,
                 placeholder: str = '?',
                 commit_fn: callable = None,
                 blob=False,
                 # @TODO Define Serde Abstract Base Class.
                 serde: 'loads/dumps' = json,
                 cachesize=2 ** 10,
                 cacheduration=3600):
        """
        If table creation is desired, the user must provide valcols. By default,
        values will be stored in text columns as json. Alternatively, the user
        may set either `blob = True` to store pickled bytes, or set `serde` to
        an object that will convert values to/from strings.


        >>> cur = get_temp_db()
        >>> cur.execute("CREATE TABLE simple (a VARCHAR, b VARCHAR, c VARCHAR, d VARCHAR)"
        ...   ).connection.commit()

        >>> simple = CompoundKeyRowDB(cur, 'simple', keycols=['a', 'b'])
        >>> simple[('foo', 'bar')] = {'c': 'knights', 'd': 'ni!'}
        >>> cur.execute("SELECT * FROM simple").fetchone()
        ('foo', 'bar', '"knights"', '"ni!"')

        >>> dropped = CompoundKeyRowDB(cur, 'simple', keycols=['a', 'b'], valcols=['different'],
        ...  drop_table=True, create_table=True)
        >>> cur.execute("SELECT * FROM simple").fetchone() is None
        True

        >>> created = CompoundKeyRowDB(cur, 'created', keycols=['a', 'b'], valcols=['spam'],
        ...   create_table=True)
        >>> created[('cam', 'elot')] = {'spam': 'alot'}

        >>> pickled = CompoundKeyRowDB(cur, 'pickled', keycols=['a', 'b', 'c'], valcols=['v'],
        ...  create_table=True, blob=True)
        >>> pickled[('gre', 'n', 'ade')] = {'v': {'count': 5}}
        >>> pickled.cache_clear()
        >>> pickled[('gre', 'n', 'ade')]['v']['count'] is 5
        True

        >>> numeric = CompoundKeyRowDB(cur, 'numeric', keycols=['a', 'b'], keytype='int',
        ...   valcols=['word', 'num'], valtype=['text', 'decimal'], create_table=True)
        >>> numeric[(1, 2)] = {'word': 'foo', 'num': 3.14}
        >>> cur.execute("SELECT a, b, word, num FROM numeric").fetchone()
        (1, 2, '"foo"', 3.14)
        """
        assert callable(serde.loads)
        assert callable(serde.dumps)

        self.keycols = keycols
        super().__init__(cursor=cursor,
                         table=table,
                         valcols=valcols,
                         keytype=keytype,
                         valtype=valtype,
                         placeholder=placeholder,
                         blob=blob,
                         create_table=create_table,
                         truncate_table=truncate_table,
                         drop_table=drop_table,
                         commit_fn=commit_fn,
                         cachesize=cachesize,
                         cacheduration=cacheduration)
        del self.keycol

    def keys(self):
        raise NotImplementedError("This class does not have method 'keys'")

    def _create_table_query(self):
        """
        Different way to define columns and set compound primary key.
        This is used inside _create_table method

        >>> cur = get_temp_db()
        >>> serde = StringSerDe()
        >>> kr = CompoundKeyRowDB(cur, 'newtable', commit_fn=cur.connection.commit,
        ...               create_table = True, keycols=['a', 'b'],
        ...               valcols=['foo', 'bar' , 'baz'], serde=serde)
        >>> kr[('spam', 'alot')] = {'foo': 'a', 'bar': 'b'}
        >>> kr.cache_clear()
        >>> kr[('spam', 'alot')] == {'foo': 'a', 'bar': 'b', 'baz': None}
        True
        """
        # make sure column types are set for every column
        self.valtype = self._check_coltypes(self.valcols, self.valtype)
        self.keytype = self._check_coltypes(self.keycols, self.keytype)
        log.debug(self.valtype)
        log.debug(self.keytype)

        columns = []
        columns += ['{} {}'.format(name, dtype) for name, dtype in self.keytype.items()]
        columns += ['{} {}'.format(name, dtype) for name, dtype in self.valtype.items()]
        query = "CREATE TABLE IF NOT EXISTS {table} ({columns}, PRIMARY KEY ({key_cols}))".format(
            table=self.table, columns=','.join(columns), key_cols=','.join(self.keycols))
        return query

    def _execute_get_query(self, key):
        """
        Different select query to filter on multiple column.
        This is used inside __getitem__ method
        """
        # @TODO Update to support varying Column Types.
        base_query = "SELECT {} from {} where {} LIMIT 1".format(
            '*' if not self.valcols else ','.join(self.valcols),
            self.table,
            " AND ".join(["{} = {}".format(c, self.placeholder) for c in self.keycols])
        )
        log.debug(base_query)
        self.db.execute(base_query, key)

    def __setitem__(self, key, values: dict):
        """
        To adapt to the fact that key is a tuple:
        - different way to make sure there is no keycol in values dict
        - different UPDATE query and exec
        - different INSERT query and exec
        """
        assert type(values) is dict

        # make sure there is no keycol in values dict
        _ = [values.pop(c, None) for c in self.keycols]
        data = [[f, self.serde.dumps(v)] for f, v in values.items()]

        try:
            values_str = ', '.join(
                ['{} = {}'.format(f, self.placeholder) for f, v in data])
            base_query = "UPDATE {} SET {}".format(self.table, values_str)
            base_query += " WHERE " + " AND ".join(["{} = {}".format(c, self.placeholder) for c in self.keycols])
            log.debug(base_query)
            self.db.execute(base_query, [v for f, v in data] + list(key))

            if self.db.rowcount < 1:
                data.extend(zip(self.keycols, key))
                base_query = "INSERT INTO {} ({}) VALUES ({})".format(
                    self.table,
                    ', '.join([f for f, v in data]),
                    ', '.join([self.placeholder for _ in data]))
                log.debug(base_query)
                self.db.execute(base_query, [v for f, v in data])

            self.cache[key] = values

            if self.commit:
                self.commit()
            return values
        except Exception as e:
            log.error('{}: {}'.format(type(e), e))
            log.error("Could not update {} => {}".format(key, values))

    def __delitem__(self, key):
        """
        Override to adopt query for mylti-column key
        """
        base_query = "DELETE from {}".format(self.table)
        base_query += " WHERE " + " AND ".join(["{} = {}".format(c, self.placeholder) for c in self.keycols])
        log.debug(base_query)
        self.db.execute(base_query, key)
        if key in self.cache:
            del (self.cache[key])
        if self.commit:
            self.commit()
        return key


class StringKeyRowDB(KeyRowDB):
    """Forces keys to be strings. Mainly for SQLite & testing."""

    def __getitem__(self, key):
        return super().__getitem__(str(key))

    def __setitem__(self, key, values):
        return super().__setitem__(str(key), values)

    def __delitem__(self, key):
        return super().__delitem__(str(key))


class UncountableKeyRowDB(KeyRowDB):
    def __len__(self):
        return 1


def get_mock(name='test', filename=None):
    cur = get_temp_db(filename)
    cur.execute(
        "CREATE TABLE {} (key VARCHAR, value VARCHAR, other VARCHAR);".format(
            name)).connection.commit()
    return KeyValueDB(cur, name, blob=False, commit_fn=cur.connection.commit)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
