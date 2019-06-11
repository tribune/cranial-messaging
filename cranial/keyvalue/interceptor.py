from cranial.keyvalue import keyvalue


class DeleteOp:
    """This class is passed as the 'value' to the intercept function for a
    __delitem__ operation. This allows the intercept function to differentiate
    a Delete from a Get."""
    pass

class GetOp:
    """ @see DeleteOp."""
    pass


class KeyValueInterceptor(keyvalue.KeyValueDB):
    ''' Adds to KeyValueDB the ability provide a "interceptor" function that can
    make modifications based on the key.

    >>> import pickle
    >>> kv = get_mock()
    >>> def fn(obj, key, value=None):
    ...     obj.valcol = 'alt' if key == 'bar' else 'value'
    ...     return key, value
    ...
    >>> kv.set_interceptor(fn)
    >>> kv['foo'] = 'a'
    >>> kv['foo']
    'a'
    >>> kv['bar'] = 'b'
    >>> kv['bar']
    'b'
    >>> list(map(lambda x: (x[0], pickle.loads(x[1]) if x[1] else x[1], pickle.loads(x[2]) if x[2] else x[2]), kv.db.execute("SELECT * FROM test").fetchall()))
    [('foo', 'a', None), ('bar', None, 'b')]
    >>> del(kv['foo'])
    >>> kv.db.connection.close()
    '''

    def set_interceptor(self, fn: callable):
        """ fn(self, key, value) -> key, value"""
        if hasattr(self, 'intercept'):
            raise Exception("Intercept function should only be set once.")
        else:
            self.intercept = fn

    def __getitem__(self, key):
        if hasattr(self, 'intercept'):
            key, _ = self.intercept(self, key, GetOp)
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        if hasattr(self, 'intercept'):
            key, value = self.intercept(self, key, value)
        return super().__setitem__(key, value)

    def __delitem__(self, key):
        if hasattr(self, 'intercept'):
            key, _ = self.intercept(self, key, DeleteOp)
        return super().__delitem__(key)


""" Module Functions. """


def get_mock():
    import sqlite3
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE test (key VARCHAR, value BLOB, alt BLOB);"
    ).connection.commit()
    return KeyValueInterceptor(cur, 'test', commit_fn=conn.commit)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
