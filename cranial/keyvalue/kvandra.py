'''
Helper functions to work with Cassandra via KeyValueDB.
'''
import json
import os

from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.query import named_tuple_factory
from cassandra.auth import PlainTextAuthProvider

from cranial.datastore.adapters.cassandra import CassandraAdapter as Adapter
from cranial.keyvalue.keyvalue import KeyValueDB as KeyValue
from cranial.common import logger

log = logger.get()
session = None


def get_cursor(existing=None, hosts=None, new=False):
    global session

    if existing:
        return existing

    if session and not new:
        return session

    if hosts is None:
        hosts = os.environ.get('KEYVALUE_HOSTS', '').split(',')

    auth = None
    try:
        with open('keys/cassandra-config.json') as f:
            auth = PlainTextAuthProvider(**json.load(f))
    except Exception as e:
        log.warning('Could not get Cassandra credentials from file.')
        log.warning(e)

    cluster = Cluster(
        hosts,
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        auth_provider=auth)

    session = cluster.connect('default')
    session.row_factory = named_tuple_factory

    return session


def get_kv(name, **kwargs):
    return KeyValue(Adapter(get_cursor()),
                    table=name,
                    create_table=True,
                    **kwargs)
