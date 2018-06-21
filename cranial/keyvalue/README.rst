KeyValueDB is intended to allow external databases to be used as a drop-in
replacement for Python Dictionaries in many use cases. Once you've initialized
a KeyValueDB object, if you know how to use a Dict, then you know how to use
it. Choosing the right sub-class and parameter options for maximum performance
for your application does require some understanding of the internals, however.

KeyValueDB is intended for rapid prototyping and optimized for ease-of-use, not
performance. Expect to need to do some refactoring, probably with a custom
sub-class, to make an application production-ready.

The first argument to a KeyValueDB init can be any DBAPI2-compatible cursor.
Since the Cassanda driver is not DBAPI2-compatible, we provide the
CassandraAdapter class which wrapps a Cassanda session and provides the DBAPI2
interface. Other database adapters in cranial are provided for convenience, and
are not necessary for use with KeyValueDB, though they may make it easier to
get a reliable cursor.

KeyValueDB does not depend on fetchers. They are mostly separate tools for
separate jobs at the present. In the future, it is conceivable that a fetcher
could make use of an optimized KeyValueDB subclass. But because fetchers are
not DBAPI2 compatible (because they are intended to support data storage
systems more generally, and not just databases), it is unlikely one would ever
want to use a fetcher inside a KeyValueDB subclass.
