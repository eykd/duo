# -*- coding: utf-8 -*-
"""tests -- Unit tests for duo.

Mocking AWS services is HARD.
"""
from __future__ import unicode_literals
from six import with_metaclass, string_types, text_type, iteritems
from six.moves import reload_module

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import datetime

import mock


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

        from boto.dynamodb import layer1
        reload_module(layer1)
        self.boto_layer1 = layer1
        # Mock out layer1 completely. This is where all the network interface occurs.
        MockLayer1 = mock.Mock(spec=layer1.Layer1)
        MockLayer1.return_value = MockLayer1

        layer1_patcher = self.layer1_patcher = mock.patch('boto.dynamodb.layer1.Layer1')
        self.mocked_layer1 = layer1_patcher.start()

        # Spy on layer2, making sure that it gets a mock layer1 object.
        from boto.dynamodb import layer2
        reload_module(layer2)
        self.boto_layer2 = layer2
        MockLayer2 = self.MockLayer2 = mock.Mock(
            spec = layer2.Layer2,
            wraps = layer2.Layer2())
        MockLayer2.layer1 = MockLayer1

        # Mock out the connection to return our special MockLayer2.
        connect_patcher = self.connect_dynamodb_patcher = mock.patch('boto.connect_dynamodb')
        self.mocked_connect_dynamodb = connect_patcher.start()
        self.mocked_connect_dynamodb.return_value = MockLayer2

        # Wire up
        self.mock_boto_tables()

    def mock_item_data(self):
        """Create a dict corresponding to an item description JSON from AWS.
        """
        data = {
            "Item": {
                self.hash_key_name: self.hash_key_value,
                self.range_key_name: self.range_key_value
            },
            "ConsumedCapacityUnits": 1
        }
        data['Item'].update(self.item_attrs)
        return data

    def mock_boto_tables(self):
        """Set up appropriate table-related stubs on the `boto.dynamodb.layer2` interface.
        """
        from boto.dynamodb import table, item
        reload_module(table)
        reload_module(item)
        self.boto_table = table
        self.boto_item = item

        # We want to be able to spy on the Table.
        MockTable = self.MockTable = mock.Mock(
            spec = table.Table,
            wraps = table.Table(self.MockLayer2, self.describe_table()))

        self.MockLayer2.describe_table.return_value = self.describe_table()
        self.MockLayer2.get_table.return_value = MockTable
        self.MockLayer2.layer1.get_item.return_value = self.mock_item_data()

        import duo
        reload_module(duo)
        self.duo = duo
        self.db = duo.DynamoDB(key=self.key, secret=self.secret)

    def describe_table(self):
        """Create a dict corresponding to a table description JSON from AWS.
        """
        return {'Table': {'CreationDateTime': 1343759006.036,
                          'ItemCount': 0,
                          'KeySchema': {'HashKeyElement': {'AttributeName': self.hash_key_name,
                                                           'AttributeType': 'S'},
                                        'RangeKeyElement': {'AttributeName': self.range_key_name,
                                                            'AttributeType': 'S'}},
                          'ProvisionedThroughput': {'ReadCapacityUnits': 10,
                                                    'WriteCapacityUnits': 5},
                          'TableName': self.table_name,
                          'TableSizeBytes': 0,
                          'TableStatus': 'ACTIVE'}}

    def tearDown(self):
        self.connect_dynamodb_patcher.stop()
        self.layer1_patcher.stop()


class DuoTests(DynamoDBTests):
    def test_connection_on_db_should_be_lazily_created(self):
        """The DB connection should be lazily created when it's needed.
        """
        self.assertEqual(self.mocked_connect_dynamodb.call_count, 0)
        self.db.connection
        self.assertEqual(self.mocked_connect_dynamodb.call_count, 1)
        self.assertTrue(self.mocked_connect_dynamodb.called_with_arguments(self.key, self.secret))

    def test_getitem_on_db_should_return_table_of_given_name(self):
        """duo.DynamoDB()[name] should return a table of the given name.
        """
        table = self.db[self.table_name]
        self.assertIs(table.table, self.MockTable)
        self.assertEqual(table.table_name, self.table_name)

    def test_getitem_on_table_should_return_item(self):
        """duo.Table()[hash_key, range_Key] should return a duo.Item().
        """
        table = self.db[self.table_name]
        item = table[self.hash_key_value, self.range_key_value]

        self.assertIsInstance(item, self.boto_item.Item)
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
