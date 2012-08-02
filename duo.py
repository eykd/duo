# -*- coding: utf-8 -*-
"""duo -- a powerful, dynamic, pythonic interface to AWS DynamoDB.
"""
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
    def __init__(self, key, secret):
        self.key = key
        self.secret = secret
        self._tables = {}
    
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
        
        table = Table._table_types[table_name](self._tables[table_name])
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


class Table(object):
    __metaclass__ = _TableMeta
    
    table_name = None
    hash_key_name = None
    range_key_name = None

    def __init__(self, table):
        self.table = table
        super(Table, self).__init__()
    
    def create(self, hash_key, range_key=None, **kwargs):
        item = self.table.new_item(
            hash_key = hash_key,
            range_key = range_key,
            attrs = kwargs,
            item_class = Item._table_types[self.table_name],
            )
        item.is_new = True
        return item

    def __getitem__(self, key):
        if isinstance(key, tuple):
            hash_key, range_key = key
        else:
            hash_key = key
            range_key = None

        if range_key is not None:
            try:
                return self.table.get_item(
                    hash_key = hash_key,
                    range_key = range_key,
                    item_class = Item._table_types[self.table_name],
                    )
            except DynamoDBKeyNotFoundError:
                return self.create(hash_key, range_key)
        else:
            return self.table.query(
                hash_key = hash_key,
                item_class = Item._table_types[self.table_name],
                )


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
        if value is 0:
            return None

        return datetime.date.fromordinal(value)

    def from_python(self, value):
        if value is None:
            return 0
        
        try:
            return value.toordinal()
        except AttributeError:
            raise ValueError('DateField requires a `datetime.date` object.')
