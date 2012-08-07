# -*- coding: utf-8 -*-
"""duo -- a powerful, dynamic, pythonic interface to AWS DynamoDB.
"""
import warnings
import collections
import datetime

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
    def __init__(self, key, secret, cache=None):
        self.key = key
        self.secret = secret
        self._tables = {}
        self.cache = cache
    
    @property
    def connection(self):
        if not hasattr(self, '_connection'):
            self._connection = boto.connect_dynamodb(
                aws_access_key_id=self.key,
                aws_secret_access_key=self.secret
                )
        return self._connection

    def reset(self):
        if hasattr(self, '_connection'):
            del self._connection
        self._tables.clear()

    def __getitem__(self, table_name):
        if table_name not in self._tables:
            self._tables[table_name] = self.connection.get_table(table_name)
        
        table = Table._table_types[table_name](self._tables[table_name], cache=self.cache)
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
    __metaclass__ = _TableMeta

    table_name = None
    cache = None
    cache_duration = None
    is_new = False

    @property
    def _cache_key(self):
        return self._duo_table._get_cache_key(self.hash_key, self.range_key)

    def _set_cache(self):
        if self.cache is not None:
            table = self._duo_table
            key = table._get_cache_key(self.hash_key, self.range_key)
            self.cache.set(key, self.items(),
                           time=self.cache_duration if self.cache_duration is not None else table.cache_duration)

    def _delete_cache(self):
        if self.cache is not None:
            table = self._duo_table
            key = table._get_cache_key(self.hash_key, self.range_key)
            self.cache.delete(key)

    def put(self, *args, **kwargs):
        result = super(Item, self).put(*args, **kwargs)
        try:
            self._set_cache()
        except Exception as e:
            warnings.warn('Cache write-through failed on put(). %s: %s' % (e.__class__.__name__, e.message))
        return result

    def save(self, *args, **kwargs):
        result = super(Item, self).save(*args, **kwargs)
        try:
            self._set_cache()
        except Exception as e:
            warnings.warn('Cache write-through failed on save(). %s: %s' % (e.__class__.__name__, e.message))
        return result

    def delete(self, *args, **kwargs):
        result = super(Item, self).delete(*args, **kwargs)
        try:
            self._delete_cache()
        except Exception as e:
            warnings.warn('Cache write-through failed on delete(). %s: %s' % (e.__class__.__name__, e.message))
        return result


class Table(object):
    __metaclass__ = _TableMeta
    
    table_name = None
    hash_key_name = None
    range_key_name = None

    cache = None
    cache_duration = 0  # Default to cache-forever
    cache_prefix = None

    def __init__(self, table, cache=None):
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
    
    def create(self, hash_key, range_key=None, **kwargs):
        item = self.table.new_item(
            hash_key = hash_key,
            range_key = range_key,
            attrs = kwargs,
            item_class = Item._table_types[self.table_name],
            )
        return self._extend(item, is_new=True)

    def _extend(self, item, is_new=False):
        item.is_new = is_new
        item.cache = self.cache
        item._duo_table = self
        return item

    def _extend_iter(self, items, is_new=False):
        for item in items:
            item.is_new = is_new
            item.cache = self.cache
            yield item

    @classmethod
    def _get_cache_key(cls, hash_key, range_key):
        if range_key is None:
            key = '%s_%s' % (cls.cache_prefix or cls.table_name, hash_key)
        else:
            key = '%s_%s_%s' % (cls.cache_prefix or cls.table_name, hash_key, range_key)
        return key

    def _get_cache(self, hash_key, range_key=None):
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
        return self.table.query(hash_key, range_key_condition=range_key_condition,
                                attributes_to_get=attributes_to_get, request_limit=request_limit,
                                max_results=max_results, consistent_read=consistent_read,
                                scan_index_forward=scan_index_forward, exclusive_start_key=exclusive_start_key,
                                item_class=Item._table_types[self.table_name])

    def scan(self, scan_filter=None, attributes_to_get=None, request_limit=None, max_results=None, count=False,
             exclusive_start_key=None):
        """Scan through this table.

        This is a very long and expensive operation, and should be avoided if at all possible.

        Returns items using the registered subclass, if one has been registered.

        See http://boto.readthedocs.org/en/latest/ref/dynamodb.html#boto.dynamodb.table.Table.scan
        """
        return self.table.scan(scan_filter=scan_filter, attributes_to_get=attributes_to_get, request_limit=request_limit,
                               max_results=max_results, count=count, exclusive_start_key=exclusive_start_key,
                               item_class=Item._table_types[self.table_name])


class NONE(object): pass


class Field(object):
    name = None
    
    def __init__(self, default=NONE, readonly=False):
        self.default = default
        self.readonly = readonly
        super(Field, self).__init__()
    
    def to_python(self, value):
        raise NotImplementedError()

    def from_python(self, value):
        raise NotImplementedError()

    def __get__(self, obj, type=None):
        try:
            value = obj[self.name]
        except KeyError:
            if self.default is not NONE:
                value = self.default
            else:
                raise AttributeError(self.name)

        return self.to_python(value)

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
                obj[self.name] = self.from_python(value)

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
    def to_python(self, value):
        return value

    def from_python(self, value):
        return unicode(value)


class IntField(Field):
    def to_python(self, value):
        return value

    def from_python(self, value):
        return int(value)


class _ChoiceMixin(Field):
    """A field mixin that enforces a set of possible values, using an Enum.
    """
    def __init__(self, **kwargs):
        self.enum_type = kwargs.pop('enum_type')
        super(_ChoiceMixin, self).__init__(**kwargs)

    def to_python(self, value):
        return self.enum_type[value]


class ChoiceField(_ChoiceMixin, UnicodeField):
    """A unicode field that enforces a set of possible values, using an Enum.
    """
    def from_python(self, value):
        return unicode(self.enum_type[value])
    

class EnumField(_ChoiceMixin, IntField):
    """An integer field that enforces a set of possible values, using an Enum.
    """
    def from_python(self, value):
        return int(self.enum_type[value])


class DateField(Field):
    def to_python(self, value):
        if value is None or value == 0:
            return None

        return datetime.date.fromordinal(value)

    def from_python(self, value):
        if value is None or value == 0:
            return 0
        
        try:
            return value.toordinal()
        except AttributeError:
            raise ValueError('DateField requires a `datetime.date` object.')
