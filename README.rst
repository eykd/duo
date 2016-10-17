Duo: A powerful, dynamic, pythonic interface to AWS DynamoDB
============================================================


.. image:: https://travis-ci.org/eykd/duo.svg?branch=master
           :target: https://travis-ci.org/eykd/duo#

.. image:: https://coveralls.io/repos/github/eykd/duo/badge.svg?branch=master
           :target: https://coveralls.io/github/eykd/duo?branch=master





Duo provides a few straightforward, Pythonic abstractions for working
with Amazon Web Services' DynamoDB. It's a very light wrapper around
`boto.dynamodb.layer2`, so you have full access to that excellent
library when you need it, but you don't have to sweat the details when
you don't.

Stern warning:
--------------

No seriously, it's a very light wrapper around
`boto.dynamodb.layer2`. If you stray much beyond the usage examples
below, you'd do best to be familiar with Boto's DynamoDB API. `The
docs`_ are excellent. Reading `duo's source`_ may also be helpful. It's
kept short for that reason.

.. _The docs: http://boto.readthedocs.org/en/latest/ref/dynamodb.html
.. _duo's source: https://github.com/eykd/duo/blob/master/duo.py


Usage:
------

`duo` is made up of one module::

    >>> import duo

The module isn't very big (at the time of this writing, ~700
lines). If you want to know how something works, `you should read it`_.

.. _you should read it: https://github.com/eykd/duo/blob/master/duo.py

Pre-create your tables in the AWS console, then write simple classes
to access them. `duo.Table` Sub-classes are automatically registered
with the db::

    >>> class MyHashKeyTable(duo.Table):
    ...     table_name = 'my_hashkey_table'
    ...     hash_key_name = 'slug'
    ...     range_key_name = None  # Implicit default


`duo.Item` is a thin wrapper around `boto.dynamodb.items.Item`, with
lots of syntactic sugar. `duo.Item` sub-classs are automatically
registered with the db::

    >>> import datetime

    >>> class MyHashKeyItem(duo.Item):
    ...     table_name = 'my_hashkey_table'
    ...     hash_key_name = 'slug'
    ...
    ...     slug = duo.UnicodeField()
    ...     my_field = duo.UnicodeField(default='foo')
    ...     on_this_date = duo.DateField(default=lambda o: datetime.date.today())


Databases and Tables use dict-like access syntax::

    >>> db = duo.DynamoDB(key='access_key', secret='secret_key')

    >>> # The correct Table sub-class is matched by table name:
    >>> table = duo.DynamoDB['my_hashkey_table']

    >>> # The correct Item sub-class is matched by table name:
    >>> item = table['new-item']

    >>> # Items are actually dict subclasses, but that's not where the
    >>> # fun is. They can only store unicode strings and integers:
    >>> item['slug']
    'new-item'


Specify a field on an Item sub-class to get useful data types::

    >>> item.is_new
    True

    >>> # A field doesn't exist initially...
    >>> item['my_field']
    Traceback (most recent call last):
      File "...", line 1, in <module>
        item['my_field']
    KeyError: 'my_field'

    >>> # But we specified a default.
    >>> item.my_field
    'foo'

    >>> # The default, once accessed, gets populated:
    >>> item['my_field']
    'foo'

    >>> # Or we can set our own value...
    >>> item.my_field = 'bar'

    >>> item['my_field']
    'bar'

    >>> # Finally, we save it to DynamoDB.
    >>> item.put()

    >>> item.is_new
    False


Caching:
--------

Duo integrates with any cache that implements a `python-memcached`\
-compatible interface, namely, the following::

    import pylibmc
    cache = pylibmc.Client(['127.0.0.1'])
    cache.get(<keyname>)
    cache.set(<keyname>, <duration-in-seconds>)
    cache.delete(<keyname>)

Integrate caching by passing the cache to the db constructor::

    >>> import duo
    >>> db = duo.DynamoDB(key='access_key', secret='secret_key', cache=cache)

You can also specify a cache object on a per-table or per-item basis::

   >>> class MyHashKeyTable(duo.Table):
    ...     cache = pylibmc.Client(['127.0.0.1'])
    ...
    ...     table_name = 'my_hashkey_table'
    ...     hash_key_name = 'slug'
    ...     range_key_name = None  # Implicit default


Caching is turned off by default, but you can turn it on by specifying
a `cache_duration` as an integer (0 is forever)::

    >>> class MyHashKeyItem(duo.Item):
    ...     cache_duration = 30  # 30 seconds
    ...
    ...     table_name = 'my_hashkey_table'
    ...     hash_key_name = 'slug'
    ...
    ...     slug = duo.UnicodeField()
    ...     my_field = duo.UnicodeField(default='foo')
    ...     on_this_date = duo.DateField(default=lambda o: datetime.date.today())


Cache keys are determined by hash key, range key, and a cache prefix
(set on the Table). By default, the cache prefix is the table name::

    >>> table = duo.DynamoDB['my_hashkey_table']
    >>> item = table['new-item']
    >>> item.cache_prefix is None
    True
    >>>item._cache_key
    'my_hashkey_table_new-item'
    >>> MyHashKeyTable.cache_prefix = 'hello_world'
    >>> item._get_cache_key()
    'hello_world_new-item'


CHANGELOG
---------

0.3.0
^^^^^

Add Python 3 compatibility.

0.2.5
^^^^^

get_item() now writes to the cache, even though it doesn't read from the cache.

0.2.4
^^^^^

Added a custom get_item to Table, for specifying consistent reads,
etc. Used by __getitem__, for simpler code!

0.2.3
^^^^^

One more packaging fix, so pip won't explode. Thanks, cbrinker!


0.2.2
^^^^^

Table.scan() and .query() should return extended Items.


0.2.1
^^^^^

Corrections/improvements to setup.py. Packaging is HARD.


0.2
^^^

Initial public release.
