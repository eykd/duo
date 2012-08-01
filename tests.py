# -*- coding: utf-8 -*-
"""tests -- Unit tests for duo.

Mocking AWS services is HARD.
"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest
    
import mock


class DynamoDBTests(unittest.TestCase):
    def setUp(self):
        self.table_name = 'test_table'
        self.hash_key_name = 'test_hash_key'
        self.range_key_name = 'test_range_key'
        self.hash_key_value = 'fred'
        self.range_key_value = 'flintstone'
        self.item_attrs = {}
        self.key = 'foo'
        self.secret = 'bar'

        from boto.dynamodb import layer1
        reload(layer1)
        self.boto_layer1 = layer1
        MockLayer1 = mock.Mock(spec=layer1.Layer1)
        MockLayer1.return_value = MockLayer1

        layer1_patcher = self.layer1_patcher = mock.patch('boto.dynamodb.layer1.Layer1')
        self.mocked_layer1 = layer1_patcher.start()

        from boto.dynamodb import layer2
        reload(layer2)
        self.boto_layer2 = layer2
        MockLayer2 = self.MockLayer2 = mock.Mock(
            spec = layer2.Layer2,
            wraps = layer2.Layer2())
        MockLayer2.layer1 = MockLayer1

        connect_patcher = self.connect_dynamodb_patcher = mock.patch('boto.connect_dynamodb')
        self.mocked_connect_dynamodb = connect_patcher.start()
        self.mocked_connect_dynamodb.return_value = MockLayer2

        self.mock()

    def mock_item_data(self):
        data = {
            "Item": {
                self.hash_key_name: self.hash_key_value,
                self.range_key_name: self.range_key_value
                },
            "ConsumedCapacityUnits": 1
            }
        data['Item'].update(self.item_attrs)
        return data

    def mock(self):
        from boto.dynamodb import table, item
        reload(table)
        reload(item)
        self.boto_table = table
        self.boto_item = item

        MockTable = self.MockTable = mock.Mock(
            spec = table.Table,
            wraps = table.Table(self.MockLayer2, self.describe_table()))
        
        self.MockLayer2.describe_table.return_value = self.describe_table()
        self.MockLayer2.get_table.return_value = MockTable
        self.MockLayer2.layer1.get_item.return_value = self.mock_item_data()

        import duo
        reload(duo)
        self.duo = duo
        self.db = duo.DynamoDB(key=self.key, secret=self.secret)

    def tearDown(self):
        self.connect_dynamodb_patcher.stop()
        self.layer1_patcher.stop()

    def describe_table(self):
        return {u'Table': {u'CreationDateTime': 1343759006.036,
                           u'ItemCount': 0,
                           u'KeySchema': {u'HashKeyElement': {u'AttributeName': self.hash_key_name,
                                                              u'AttributeType': u'S'},
                                          u'RangeKeyElement': {u'AttributeName': self.range_key_name,
                                                               u'AttributeType': u'S'}},
                           u'ProvisionedThroughput': {u'ReadCapacityUnits': 10,
                                                      u'WriteCapacityUnits': 5},
                           u'TableName': self.table_name,
                           u'TableSizeBytes': 0,
                           u'TableStatus': u'ACTIVE'}}

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
