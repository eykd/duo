# -*- coding: utf-8 -*-
"""tests -- Unit tests for duo.

Mocking AWS services is HARD. Moto is easy.
"""
from __future__ import unicode_literals
from six import with_metaclass, string_types, text_type, iteritems

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import datetime

import boto
import moto



class DynamoDBTests(unittest.TestCase):
    # Default settings for describing the table we want to work with,
    # in lieu of actual values from AWS.
    default_item_data = dict(
        table_name = 'test_table',
        hash_key_name = 'test_hash_key',
        range_key_name = 'test_range_key',
        hash_key_value = 'fred',
        range_key_value = 'flintstone',
        item_attrs = {},
        key = 'foo',
        secret = 'bar',
    )

    def setUp(self):
        super(DynamoDBTests, self).setUp()
        for key, value in iteritems(self.default_item_data):
            setattr(self, key, value)

        self.dynamo_patcher = moto.mock_dynamodb()
        self.dynamo_patcher.start()

        import duo
        self.duo = duo
        self.db = duo.DynamoDB(key=self.key, secret=self.secret)
        self.schema = self.db.connection.create_schema(
            hash_key_name=self.hash_key_name,
            hash_key_proto_value=str,
            range_key_name=self.range_key_name,
            range_key_proto_value=str,
        )
        self.db.connection.create_table(self.table_name, self.schema, 10, 10)

    def tearDown(self):
        self.dynamo_patcher.stop()


class DuoTests(DynamoDBTests):
    def test_getitem_on_db_should_return_table_of_given_name(self):
        """duo.DynamoDB()[name] should return a table of the given name.
        """
        table = self.db[self.table_name]
        self.assertIsInstance(table.table, boto.dynamodb.table.Table)
        self.assertEqual(table.table_name, self.table_name)

    def test_getitem_on_table_should_return_item(self):
        """duo.Table()[hash_key, range_Key] should return a duo.Item().
        """
        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, boto.dynamodb.item.Item)
        self.assertIsInstance(item, self.duo.Item)
        self.assertEqual(item[self.hash_key_name], self.hash_key_value)

    def test_getitem_on_table_with_registered_item_subclass_should_return_subclass(self):
        """duo.Table()[hash_key, range_Key] should return the registered duo.Item subclass.
        """
        class TestItemSubclass(self.duo.Item):
            table_name = self.table_name

        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, TestItemSubclass)
        self.assertEqual(item[self.hash_key_name], self.hash_key_value)

    def test_unicode_fields_should_always_cast_to_unicode(self):
        class TestItemSubclass(self.duo.Item):
            table_name = self.table_name

            foo = self.duo.UnicodeField()

        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, TestItemSubclass)
        self.assertEqual(item[self.hash_key_name], self.hash_key_value)

        item.foo = 'bar'
        self.assertIsInstance(item['foo'], string_types)
        self.assertEqual(item['foo'], 'bar')
        self.assertEqual(item.foo, 'bar')

        item.foo = 9
        self.assertIsInstance(item['foo'], string_types)
        self.assertEqual(item['foo'], '9')
        self.assertEqual(item.foo, '9')

    def test_integer_fields_should_always_cast_to_an_integer(self):
        class TestItemSubclass(self.duo.Item):
            table_name = self.table_name

            foo = self.duo.IntField()

        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, TestItemSubclass)
        self.assertEqual(item[self.hash_key_name], self.hash_key_value)

        with self.assertRaises(ValueError):
            item.foo = 'bar'

        item.foo = 9
        self.assertIsInstance(item['foo'], int)
        self.assertEqual(item['foo'], 9)

    def test_date_fields_should_always_cast_to_an_integer(self):
        class TestItemSubclass(self.duo.Item):
            table_name = self.table_name

            foo = self.duo.DateField()

        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, TestItemSubclass)
        self.assertEqual(item[self.hash_key_name], self.hash_key_value)

        with self.assertRaises(ValueError):
            item.foo = 'bar'

        item.foo = today = datetime.date.today()
        self.assertIsInstance(item['foo'], int)
        self.assertEqual(item['foo'], today.toordinal())
        self.assertEqual(item.foo, today)

    def test_date_fields_should_accept_None_as_a_null_value(self):
        class TestItemSubclass(self.duo.Item):
            table_name = self.table_name

            foo = self.duo.DateField()

        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, TestItemSubclass)
        self.assertEqual(item[self.hash_key_name], self.hash_key_value)

        item.foo = None
        self.assertRaises(KeyError, item.__getitem__, 'foo')
        self.assertEqual(item.foo, None)

    def test_date_fields_should_work_with_default_of_None(self):
        class TestItemSubclass(self.duo.Item):
            table_name = self.table_name

            foo = self.duo.DateField(default=None)

        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, TestItemSubclass)
        self.assertEqual(item[self.hash_key_name], self.hash_key_value)

        self.assertFalse('foo' in item)
        self.assertEqual(item.foo, None)

    def test_enum_classes_should_integrate_subclasses_as_enumerations(self):
        class Placeholder(with_metaclass(self.duo.EnumMeta, object)): pass

        class Foo(Placeholder): pass

        class Bar(Placeholder): pass

        class Baz(Placeholder): pass

        self.assertEqual(int(Foo), 0)
        self.assertEqual(text_type(Foo), 'Foo')
        self.assertIs(Placeholder[0], Foo)
        self.assertIs(Placeholder['Foo'], Foo)
        self.assertIs(Placeholder.Foo, Foo)

        self.assertEqual(int(Bar), 1)
        self.assertEqual(text_type(Bar), 'Bar')
        self.assertIs(Placeholder[1], Bar)
        self.assertIs(Placeholder['Bar'], Bar)
        self.assertIs(Placeholder.Bar, Bar)

        self.assertEqual(int(Baz), 2)
        self.assertEqual(text_type(Baz), 'Baz')
        self.assertIs(Placeholder[2], Baz)
        self.assertIs(Placeholder['Baz'], Baz)
        self.assertIs(Placeholder.Baz, Baz)

    def test_choice_fields_should_always_cast_to_unicode(self):
        class Placeholder(with_metaclass(self.duo.EnumMeta, object)): pass

        class Foo(Placeholder): pass

        class Bar(Placeholder): pass

        class Baz(Placeholder): pass

        class TestItemSubclass(self.duo.Item):
            table_name = self.table_name

            place = self.duo.ChoiceField(enum_type=Placeholder)

        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, TestItemSubclass)
        self.assertEqual(item[self.hash_key_name], self.hash_key_value)

        with self.assertRaises(KeyError):
            item.place = 'bar'

        item.place = 'Bar'
        self.assertIsInstance(item['place'], string_types)
        self.assertEqual(item['place'], 'Bar')
        self.assertIs(item.place, Bar)

    def test_enum_fields_should_always_cast_to_an_int(self):
        class Placeholder(with_metaclass(self.duo.EnumMeta, object)): pass

        class Foo(Placeholder): pass

        class Bar(Placeholder): pass

        class Baz(Placeholder): pass

        class TestItemSubclass(self.duo.Item):
            table_name = self.table_name

            place = self.duo.EnumField(enum_type=Placeholder)

        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, TestItemSubclass)
        self.assertEqual(item[self.hash_key_name], self.hash_key_value)

        with self.assertRaises(KeyError):
            item.place = 'bar'

        item.place = 'Bar'
        self.assertIsInstance(item['place'], int)
        self.assertEqual(item['place'], 1)
        self.assertIs(item.place, Bar)

    def test_enum_classes_should_compare(self):
        class Placeholder(with_metaclass(self.duo.EnumMeta, object)): pass

        class Foo(Placeholder): pass

        class Bar(Placeholder): pass

        class Baz(Placeholder): pass

        self.assertEqual(Foo, 0)
        self.assertEqual(Foo, 'Foo')
        self.assertEqual(Foo, Foo)
        self.assertLess(Foo, Bar)
        self.assertEqual(Bar, 1)
        self.assertEqual(Bar, 'Bar')
        self.assertEqual(Bar, Bar)
        self.assertGreater(Bar, Foo)
        self.assertLess(Bar, Baz)
        self.assertEqual(Baz, 2)
        self.assertEqual(Baz, 'Baz')
        self.assertEqual(Baz, Baz)
        self.assertGreater(Baz, Bar)
        self.assertLess(Baz, 3)
