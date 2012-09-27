# -*- coding: utf-8 -*-
"""duo -- a powerful, dynamic, pythonic interface to AWS DynamoDB.
"""
import warnings
import collections
import datetime
import time
import json

import boto
from boto.dynamodb.item import Item as _Item
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError


class EnumMeta(type):
    """Simple metaclass for enumerated types.

    Set the metaclass on a new class, then subclass that class to
    create new members of the enumerated type.
    """
    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, 'members'):
            # This branch only executes when processing the mount point itself.
            # So, since this is a new plugin type, not an implementation, this
            # class shouldn't be registered as a plugin. Instead, it sets up a
            # list where plugins can be registered later.
            cls.members = []
        else:
            # This must be a plugin implementation, which should be registered.
            # Simply appending it to the list is all that's needed to keep
            # track of it later.
            cls.members.append(cls)
            cls.index = len(cls.members) - 1
            setattr(cls.__class__, cls.__name__, cls)
            cls.key = cls.__name__

    def __iter__(cls):
        return iter(cls.members)

    def __len__(cls):
        return len(cls.members)

    def __getitem__(cls, idx):
        try:
            if isinstance(idx, EnumMeta):
                return cls.members[int(idx)]
            elif isinstance(idx, int):
                return cls.members[idx]
            elif isinstance(idx, basestring):
                try:
                    return getattr(cls, idx)
                except AttributeError:
                    pass
        except KeyError:
            raise TypeError("'%s' does not support indexing." % cls.__name__)

        # Failing all that, raise our own KeyError.
        raise KeyError(idx)

    def __int__(cls):
        try:
            return cls.index
        except AttributeError:
            raise ValueError("'%s' does not support integer casting.")

    def __nonzero__(cls):
        return bool(int(cls))

    def __cmp__(self, other):
        if isinstance(other, basestring):
            return cmp(str(self), other)
        else:
            return cmp(int(self), other)

    def __str__(cls):
        try:
            return cls.key
        except AttributeError:
            return super(EnumMeta, cls).__str__()

    def __unicode__(cls):
        try:
            return unicode(cls.key)
        except AttributeError:
            return super(EnumMeta, cls).__unicode__()


class DynamoDB(object):
    """DB object for managing a connection to DynamoDB and looking up custom Table handlers.
    """
    def __init__(self, key, secret, cache=None):
        self.key = key
        self.secret = secret
        self._tables = {}
        self.cache = cache
    
    @property
    def connection(self):
        """Lazy-load a boto DynamoDB connection.
        """
        if not hasattr(self, '_connection'):
            self._connection = boto.connect_dynamodb(
                aws_access_key_id=self.key,
                aws_secret_access_key=self.secret
                )
        return self._connection

    def reset(self):
        """Reset the DynamoDB connection and clear any cached tables.
        """
        if hasattr(self, '_connection'):
            del self._connection
        self._tables.clear()

    def __getitem__(self, table_name):
        if hasattr(table_name, 'table_name'):
            table_name = table_name.table_name
        
        if table_name not in self._tables:
            self._tables[table_name] = self.connection.get_table(table_name)
        
        table = Table._table_types[table_name](self, self._tables[table_name], cache=self.cache)
        table.table_name = table_name
        return table


class _TableMeta(type):
    """Metaclass plugin mount for plugins related to AWS DynamoDB tables.
    """
    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, '_table_types'):
            # This branch only executes when processing the mount point itself.
            # So, since this is a new plugin type, not an implementation, this
            # class shouldn't be registered as a plugin. Instead, it sets up a
            # list where plugins can be registered later.
            cls._table_types = collections.defaultdict(lambda: cls)
        else:
            # This must be a plugin implementation, which should be registered.
            # Simply appending it to the list is all that's needed to keep
            # track of it later.
            cls._table_types[cls.table_name] = cls
            for name, value in attrs.copy().iteritems():
                if isinstance(value, Field):
                    value.name = name


class Item(_Item):
    """A boto DynamoDB Item, with caching secret sauce.

    Subclass to customize fields and caching behavior. Subclassing
    auto-registers with the DB.
    """
    __metaclass__ = _TableMeta

    duo_db = None
    duo_table = None
    
    table_name = None
    cache = None
    cache_duration = None
    is_new = False

    def __init__(self, *args, **kwargs):
        super(Item, self).__init__(*args, **kwargs)
        self._original = self.copy()

    @property
    def dynamo_key(self):
        """Return the hash_key or (hash_key, range_key) key.

        The returned value is suitable for looking up the item in the
        table via __getitem__(key)
        """
        if self.range_key_name is None:
            return self.hash_key
        else:
            return (self.hash_key, self.range_key)

    @property
    def _cache_key(self):
        """Determine the key for accessing the item in the cache.
        """
        return self.duo_table._get_cache_key(self.hash_key, self.range_key)

    def _set_cache(self):
        """Store the item in the cache.
        """
        if self.cache is not None and self.cache_duration is not None:
            table = self.duo_table
            key = table._get_cache_key(self.hash_key, self.range_key)
            duration = self.cache_duration if self.cache_duration is not None else table.cache_duration
            self.cache.set(key, self.items(), duration)

    def _delete_cache(self):
        """Remove the item from the cache.
        """
        if self.cache is not None:
            table = self.duo_table
            key = table._get_cache_key(self.hash_key, self.range_key)
            self.cache.delete(key)

    def get_expected(self):
        """Get a dictionary of original values for the object, with new attributes filled in w/ False.

        This is useful for the `expected_value` argument to put/save.
        """
        expected = {}
        for key in self.items():
            expected[key] = False
        expected.update(self._original)
        return expected

    def put(self, *args, **kwargs):
        """Put the item in the database, and also in the cache.
        """
        result = super(Item, self).put(*args, **kwargs)
        self.is_new = False
        try:
            self._set_cache()
        except Exception as e:
            warnings.warn('Cache write-through failed on put(). %s: %s' % (e.__class__.__name__, e.message))
        return result

    def put_conditionally(self, *args, **kwargs):
        """Put the item in the database, but only if the original values still hold.
        """
        kwargs['expected_value'] = self.get_expected()
        return self.put(*args, **kwargs)

    def save(self, *args, **kwargs):
        """Save the item in the database, and also in the cache.
        """
        result = super(Item, self).save(*args, **kwargs)
        self.is_new = False
        try:
            self._set_cache()
        except Exception as e:
            warnings.warn('Cache write-through failed on save(). %s: %s' % (e.__class__.__name__, e.message))
        return result

    def save_conditionally(self, *args, **kwargs):
        """Save the updated item in the database, but only if the original values still hold.
        """
        kwargs['expected_value'] = self.get_expected()
        return self.save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        """Delete the item from the database, and also from the cache.
        """
        result = super(Item, self).delete(*args, **kwargs)
        self.is_new = True
        try:
            self._delete_cache()
        except Exception as e:
            warnings.warn('Cache write-through failed on delete(). %s: %s' % (e.__class__.__name__, e.message))
        return result


class Table(object):
    """A DynamoDB Table, with super dict-like powers.

    Subclass to customize behavior. Subclassing auto-registers with
    the DB.
    """
    __metaclass__ = _TableMeta
    
    table_name = None
    hash_key_name = None
    range_key_name = None

    cache = None
    cache_prefix = None

    def __init__(self, db, table, cache=None):
        self.duo_db = db
        self.table = table
        if self.cache is None:
            self.cache = cache
        super(Table, self).__init__()

    def keys(self):
        """Return an iterator of object keys, either by `hash_key` or `(hash_key, range_key)`.

        WARNING: This performs a table scan, which can be expensive on a large table.
        """
        if self.range_key_name is None:
            return (i[self.hash_key_name] for i in self.scan(attributes_to_get=[self.hash_key_name]))
        else:
            return ((i[self.hash_key_name], i[self.range_key_name])
                    for i in self.scan(attributes_to_get=[self.hash_key_name, self.range_key_name]))

    def items(self):
        """Return an iterator of object key/value pairs, either by `hash_key` or `(hash_key, range_key)`.

        WARNING: This performs a table scan, which can be expensive on a large table.
        """
        if self.range_key_name is None:
            return ((i[self.hash_key_name], i) for i in self.scan())
        else:
            return (((i[self.hash_key_name], i[self.range_key_name]), i)
                    for i in self.scan())

    def values(self):
        """Return an iterator of objects in the table.

        Equivalent of `.scan()` sans arguments.

        WARNING: This performs a table scan, which can be expensive on a large table.
        """
        return self.scan()

    def create(self, hash_key, range_key=None, **kwargs):
        """Create an item given the specified attributes.
        """
        item = self.table.new_item(
            hash_key = hash_key,
            range_key = range_key,
            attrs = kwargs,
            item_class = Item._table_types[self.table_name],
            )
        return self._extend(item, is_new=True)

    def _extend(self, item, is_new=False):
        """Extend the given Item with some necessary attributes.
        """
        item.is_new = is_new
        item.cache = self.cache
        item.duo_table = self
        item.duo_db = self.duo_db
        return item

    def _extend_iter(self, items, is_new=False):
        """Extend a collection of Items with some necessary attributes.
        """
        for item in items:
            yield self._extend(item, is_new)

    @classmethod
    def _get_cache_key(cls, hash_key, range_key):
        """Determine the cache key for a given table key.

        Specify `range_key=None` for a hash-only key.
        """
        if range_key is None:
            key = '%s_%s' % (cls.cache_prefix or cls.table_name, hash_key)
        else:
            key = '%s_%s_%s' % (cls.cache_prefix or cls.table_name, hash_key, range_key)
        return key

    def _get_cache(self, hash_key, range_key=None):
        """Retrieve the specified item from the cache, if available.
        """
        if self.cache is None:
            return None
        else:
            key = self._get_cache_key(hash_key, range_key)
            cached = self.cache.get(key)
            if cached is not None:
                # Build an Item.
                cached = self._extend(
                    Item._table_types[self.table_name](
                        self.table,
                        hash_key = hash_key,
                        range_key = range_key,
                        attrs = dict(cached)
                        ))
            return cached

    def __getitem__(self, key):
        if isinstance(key, tuple):
            hash_key, range_key = key
        else:
            hash_key = key
            range_key = None

        # Check the cache first.
        cached = self._get_cache(hash_key, range_key)
        if cached is not None:
            return cached

        try:
            if range_key is None:
                if self.range_key_name is None:
                    item = self._extend(
                        self.table.get_item(
                            hash_key = hash_key,
                            item_class = Item._table_types[self.table_name],
                            ))
                else:
                    return self._extend_iter(self.query(hash_key))
            else:
                item = self._extend(
                    self.table.get_item(
                        hash_key = hash_key,
                        range_key = range_key,
                        item_class = Item._table_types[self.table_name],
                        ))
        except DynamoDBKeyNotFoundError:
            item = self.create(hash_key, range_key)

        if hasattr(item, 'is_new') and not item.is_new:
            item._set_cache()

        return item

    def query(self, hash_key, range_key_condition=None,
              attributes_to_get=None, request_limit=None,
              max_results=None, consistent_read=False,
              scan_index_forward=True, exclusive_start_key=None):
        """Perform a query on the table.

        Returns items using the registered subclass, if one has been registered.

        See http://boto.readthedocs.org/en/latest/ref/dynamodb.html#boto.dynamodb.table.Table.query
        """
        return self._extend_iter(
            self.table.query(hash_key, range_key_condition=range_key_condition,
                             attributes_to_get=attributes_to_get, request_limit=request_limit,
                             max_results=max_results, consistent_read=consistent_read,
                             scan_index_forward=scan_index_forward, exclusive_start_key=exclusive_start_key,
                             item_class=Item._table_types[self.table_name]))

    def scan(self, scan_filter=None, attributes_to_get=None, request_limit=None, max_results=None, count=False,
             exclusive_start_key=None):
        """Scan through this table.

        This is a very long and expensive operation, and should be avoided if at all possible.

        Returns items using the registered subclass, if one has been registered.

        See http://boto.readthedocs.org/en/latest/ref/dynamodb.html#boto.dynamodb.table.Table.scan
        """
        return self._extend_iter(
            self.table.scan(scan_filter=scan_filter, attributes_to_get=attributes_to_get, request_limit=request_limit,
                            max_results=max_results, count=count, exclusive_start_key=exclusive_start_key,
                            item_class=Item._table_types[self.table_name]))
    
    
class NONE(object): pass


class Field(object):
    """A Field acts as a data descriptor on Item subclasses.
    """
    name = None
    
    def __init__(self, default=NONE, readonly=False):
        self.default = default
        self.readonly = readonly
        super(Field, self).__init__()
    
    def to_python(self, obj, value):
        raise NotImplementedError()

    def from_python(self, obj, value):
        raise NotImplementedError()

    def __get__(self, obj, type=None):
        try:
            value = self.to_python(obj, obj[self.name])
        except KeyError:
            if self.default is not NONE:
                if callable(self.default) and not isinstance(self.default, EnumMeta):
                    value = self.default(obj)
                else:
                    value = self.default
                value = self.to_python(obj, value)
                if value:
                    # Populate the default on the object.
                    setattr(obj, self.name, value)
            else:
                return None

        return value

    def __set__(self, obj, value):
        if self.name == getattr(obj, 'hash_key_name'):
            raise AttributeError('Cannot set hash key `%s`!' % self.name)
        elif self.name == getattr(obj, 'range_key_name'):
            raise AttributeError('Cannot set range key `%s`!' % self.name)
        elif self.readonly:
            raise AttributeError('`%s` is read-only!' % self.name)
        else:
            if value is None:
                # If value is None and the attribute exists, clear it.
                if self.name in obj:
                    del obj[self.name]
            else:
                obj[self.name] = self.from_python(obj, value)

    def __delete__(self, obj):
        if self.name == getattr(obj, 'hash_key_name'):
            raise AttributeError('Cannot delete hash key `%s`!' % self.name)
        elif self.name == getattr(obj, 'range_key_name'):
            raise AttributeError('Cannot delete range key `%s`!' % self.name)
        elif self.readonly:
            raise AttributeError('`%s` is read-only!' % self.name)
        else:
            del obj[self.name]


class UnicodeField(Field):
    """Store a simple unicode string as a native DynamoDB string.
    """
    def to_python(self, obj, value):
        return value

    def from_python(self, obj, value):
        return unicode(value)


class IntegerField(Field):
    """Store a simple integer as a native DynamoDB integer.
    """
    def to_python(self, obj, value):
        return value

    def from_python(self, obj, value):
        return int(value)


IntField = IntegerField


class _ChoiceMixin(Field):
    """A field mixin that enforces a set of possible values, using an Enum.
    """
    def __init__(self, **kwargs):
        self.enum_type = kwargs.pop('enum_type')
        super(_ChoiceMixin, self).__init__(**kwargs)

    def to_python(self, obj, value):
        return self.enum_type[value]


class ChoiceField(_ChoiceMixin, UnicodeField):
    """A unicode field that enforces a set of possible values, using an Enum.
    """
    def from_python(self, obj, value):
        return unicode(self.enum_type[value])
    

class EnumField(_ChoiceMixin, IntField):
    """An integer field that enforces a set of possible values, using an Enum.
    """
    def from_python(self, obj, value):
        return int(self.enum_type[value])


class DateField(Field):
    """An integer field that stores `datetime.date` objects as ordinal integers.
    """
    def to_python(self, obj, value):
        if value is None or value == 0:
            return None

        return datetime.date.fromordinal(value)

    def from_python(self, obj, value):
        if value is None or value == 0:
            return 0
        
        try:
            return value.toordinal()
        except AttributeError:
            raise ValueError('DateField requires a `datetime.date` object.')


class DateTimeField(Field):
    """An integer field that stores `datetime.datedatetime` objects as unix timestamps.
    """
    def to_python(self, obj, value):
        if value is None or value == 0:
            return None

        return datetime.datetime.fromtimestamp(value)

    def from_python(self, obj, value):
        if value is None or value == 0:
            return 0
        
        try:
            return time.mktime(value.timetuple())
        except AttributeError:
            raise ValueError('DateTimeField requires a `datetime.datetime` object.')


class ForeignKeyField(Field):
    """A unicode field that stores foreign DynamoDB table references as a JSON-serialized string.
    """
    def to_python(self, obj, value):
        if isinstance(value, Item):
            return value
        
        elif isinstance(value, dict):
            fk_dict = value        
        else:
            fk_dict = json.loads(value)
        table_name = fk_dict['table']
        key = fk_dict['key']
        if isinstance(key, list):
            key = tuple(key)
        table = obj.duo_db[table_name]
        return table[key]

    def from_python(self, obj, value):
        return json.dumps({
            'table': value.table_name,
            'key': value.dynamo_key
            })
