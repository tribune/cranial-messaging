import importlib

class Param:
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return 'Param(%r)' % (self.value,)

def to_qmark(chunks):
    query_parts = []
    params = []
    for chunk in chunks:
        if isinstance(chunk, Param):
            params.append(chunk.value)
            query_parts.append('?')
        else:
            query_parts.append(chunk)
    return ''.join(query_parts), params

def to_numeric(chunks):
    query_parts = []
    params = []
    for chunk in chunks:
        if isinstance(chunk, Param):
            params.append(chunk.value)
            query_parts.append(':%d' % len(params))
        else:
            query_parts.append(chunk)
    return ''.join(query_parts), tuple(params)  # DCOracle2 has broken support
                                                # for sequences of other types
def to_named(chunks):
    query_parts = []
    params = {}
    for chunk in chunks:
        if isinstance(chunk, Param):
            name = 'p%d' % len(params)  # Are numbers in name allowed?
            params[name] = chunk.value
            query_parts.append(':%s' % name)
        else:
            query_parts.append(chunk)
    return ''.join(query_parts), params

def to_format(chunks):
    query_parts = []
    params = []
    for chunk in chunks:
        if isinstance(chunk, Param):
            params.append(chunk.value)
            query_parts.append('%s')
        else:
            query_parts.append(chunk.replace('%', '%%'))
    return ''.join(query_parts), params

def to_pyformat(chunks):
    query_parts = []
    params = {}
    for chunk in chunks:
        if isinstance(chunk, Param):
            name = '%d' % len(params)
            params[name] = chunk.value
            query_parts.append('%%(%s)s' % name)
        else:
            query_parts.append(chunk.replace('%', '%%'))
    return ''.join(query_parts), params


def get_paramstyle(conn):
    """ This should work with a connection or cursor object."""
    name = conn.__class__.__module__.split('.')[0]
    mod = importlib.import_module(name)
    return mod.paramstyle


def render_params(conn, chunks):
    try:
        style = get_paramstyle(conn)
    except:
        style = 'format'

    return globals()['to_' + style](chunks)


def get_temp_db(filename=None):
    import sqlite3
    if filename is None:
        conn = sqlite3.connect(":memory:")
    else:
        conn = sqlite3.connect(filename)
    return conn.cursor()
