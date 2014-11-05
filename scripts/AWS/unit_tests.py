import unittest
from boto.dynamodb2.layer1 import DynamoDBConnection
from events import *
from query_filter import QueryFilter
from selectors import *
from tables import *


class TestQueryFilter(unittest.TestCase):
    def setUp(self):
        self.query_filter = QueryFilter()

    def test_equal(self):
        self.query_filter.add('event_type', SelectorEqual('INFO'))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'event_type__EQ': 'INFO'
                             })

    def test_notequal(self):
        self.query_filter.add('event_type', SelectorNotEqual('ERROR'))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'event_type__NE': 'ERROR'
                             })

    def test_lessthan(self):
        self.query_filter.add('timestamp', SelectorLessThan(123))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'timestamp__LT': 123
                             })

    def test_greaterthan(self):
        self.query_filter.add('timestamp', SelectorGreaterThan(400))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'timestamp__GT': 400
                             })

    def test_lessthanequal(self):
        self.query_filter.add('timestamp', SelectorLessThanEqual(500))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'timestamp__LE': 500
                             })

    def test_greaterthanequal(self):
        self.query_filter.add('timestamp', SelectorGreaterThanEqual(600))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'timestamp__GE': 600
                             })


class TestTables(unittest.TestCase):
    def setUp(self):
        self.connection = DynamoDBConnection(host='localhost',
                                             port=8000,
                                             aws_secret_access_key='unittest',
                                             aws_access_key_id='unittest',
                                             region='us-east-1',
                                             is_secure=False)
        TelemetryTable.PREFIX = 'unittest'

    def test_log_table(self):
        # Create the able
        LogTable.create_table(self.connection)

        # Create items
        logs = [
            {
                'id_': '1',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-11-01T08:00:00Z'),
                'event_type': 'INFO',
                'message': 'info log #1'
            },
            {
                'id_': '2',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-11-01T08:00:00Z'),
                'event_type': 'INFO',
                'message': 'info log #2'
            },
            {
                'id_': '3',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-11-01T08:00:05Z'),
                'event_type': 'WARN',
                'message': 'warn log #3'
            },
            {
                'id_': '4',
                'client': 'john',
                'timestamp': UtcDateTime('2014-11-01T08:00:10Z'),
                'event_type': 'ERROR',
                'message': 'error log #4'
            },
            {
                'id_': '5',
                'client': 'john',
                'timestamp': UtcDateTime('2014-11-01T09:00:11Z'),
                'event_type': 'INFO',
                'message': 'info log #5'
            }
        ]
        for log in logs:
            event = LogEvent(self.connection, **log)
            event.save()

        # Scan all
        events = LogEvent.scan(self.connection)
        for event in events:
            print event['id'], event['timestamp']
            index = int(event['id']) - 1

        # Query them back
        # Query by Id


        # Delete some items

        # Make sure query now returns no match



    def test_wbxml_table(self):
        pass

    def test_counter_table(self):
        pass

    def test_capture_table(self):
        pass

    def test_support_table(self):
        pass

    def test_ui_table(self):
        pass


if __name__ == '__main__':
    unittest.main()