import unittest
from boto.dynamodb2.layer1 import DynamoDBConnection
from events import *
from selectors import *
from tables import *
from query import Query


class TestQueryFilter(unittest.TestCase):
    def setUp(self):
        self.query_filter = QueryFilter()

    def test_equal(self):
        self.query_filter.add('event_type', SelectorEqual('INFO'))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'event_type__eq': 'INFO'
                             })

    def test_notequal(self):
        self.query_filter.add('event_type', SelectorNotEqual('ERROR'))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'event_type__ne': 'ERROR'
                             })

    def test_lessthan(self):
        self.query_filter.add('timestamp', SelectorLessThan(123))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'timestamp__lt': 123
                             })

    def test_greaterthan(self):
        self.query_filter.add('timestamp', SelectorGreaterThan(400))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'timestamp__gt': 400
                             })

    def test_lessthanequal(self):
        self.query_filter.add('timestamp', SelectorLessThanEqual(500))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'timestamp__le': 500
                             })

    def test_greaterthanequal(self):
        self.query_filter.add('timestamp', SelectorGreaterThanEqual(600))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'timestamp__ge': 600
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

        # Delete all tables
        tables = self.connection.list_tables()['TableNames']
        for table in tables:
            self.connection.delete_table(str(table))

        # Create them all
        for event_cls in Query.EVENT_CLASSES:
            table_cls = event_cls.TABLE_CLASS
            table_cls.create_table(self.connection)

    def compare_events(self, expected, got):
        for (field, value) in expected.items():
            if field[-1] == '_':
                field = field[:-1]  # remove trailing '_' (used to avoid python keyword conflict)
            self.assertEquals(got[field], value, field)

    def generic_tests(self, items, event_cls):
        """
        Insert all items to db. Scan all of them. Query by id on all of them.
        :param items:
        :param event_cls:
        :return:
        """
        # Insert them into db
        for item in items:
            event = event_cls(self.connection, **item)
            event.save()

        # Scan and verify them
        events = event_cls.scan(self.connection)
        for event in events:
            print str(event) + '\n'
            index = int(event['id']) - 1
            self.compare_events(items[index], event)

        # Query by id
        for index in range(len(items)):
            id_ = index + 1
            query = Query()
            query.add('id', SelectorEqual(str(id_)))
            events = Query.events(query, self.connection)
            self.assertEqual(len(events), 1)
            self.compare_events(items[index], events[0])

    def test_log_table(self):
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
        self.generic_tests(logs, LogEvent)

        # Query by client id
        query = Query()
        query.add('client', SelectorEqual('bob'))
        events = Query.events(query, self.connection)
        self.assertEqual(3, len(events))
        self.compare_events(logs[0], events[0])
        self.compare_events(logs[1], events[1])
        self.compare_events(logs[2], events[2])

        query = Query()
        query.add('client', SelectorEqual('john'))
        events = Query.events(query, self.connection)
        self.assertEqual(2, len(events))
        self.compare_events(logs[3], events[0])
        self.compare_events(logs[4], events[1])

        # Query by client id + timestamp
        query = Query()
        query.add('client', SelectorEqual('bob'))
        query.add('timestamp', SelectorEqual(UtcDateTime('2014-11-01T08:00:05Z')))
        events = Query.events(query, self.connection)
        self.assertEqual(1, len(events))
        self.compare_events(logs[2], events[0])

        # Query by client id + timestamp range
        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorLessThan(UtcDateTime('2014-11-01T08:30:00Z')))
        events = Query.events(query, self.connection)
        self.assertEqual(1, len(events))
        self.compare_events(logs[3], events[0])

        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorGreaterThan(UtcDateTime('2014-11-01T08:30:00Z')))
        events = Query.events(query, self.connection)
        self.assertEqual(1, len(events))
        self.compare_events(logs[4], events[0])

        # Query by event_type

        # Query by event_type + timestamp

        # Query by event_type + timestamp range

        # Query by timestamp range (should fall back to a scan)


    def test_wbxml_table(self):
        pass

    def test_counter_table(self):
        # Create items
        counters = [
            {
                'id_': '1',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-10-17T01:00:00Z'),
                'counter_name': 'counter #1',
                'count': 101,
                'counter_start': UtcDateTime('2014-10-17T01:00:00Z'),
                'counter_end': UtcDateTime('2014-10-17T01:10:00Z')
            },
            {
                'id_': '2',
                'client': 'john',
                'timestamp': UtcDateTime('2014-10-17T02:00:00Z'),
                'counter_name': u'counter #2',
                'count': 1001,
                'counter_start': UtcDateTime('2014-10-17T02:00:00Z'),
                'counter_end': UtcDateTime('2014-10-17T02:01:00Z')
            },
            {
                'id_': '3',
                'client': 'john',
                'timestamp': UtcDateTime('2014-10-17T03:00:00Z'),
                'counter_name': 'counter #3',
                'count': 201,
                'counter_start': UtcDateTime('2014-10-17T03:00:00Z'),
                'counter_end': UtcDateTime('2014-10-17T03:01:00Z')
            }
        ]
        self.generic_tests(counters, CounterEvent)

    def test_capture_table(self):
        # Create items
        captures = [
            {
                'id_': '1',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-09-01T00:00:00Z'),
                'capture_name': 'capture #1',
                'count': 100,
                'min_': 100,
                'max_': 200,
                'average': 150,
                'stddev': 50
            },
            {
                'id_': '2',
                'client': 'john',
                'timestamp': UtcDateTime('2014-09-01T01:00:00Z'),
                'capture_name': 'capture #2',
                'count': 50,
                'min_': 1000,
                'max_': 1050,
                'average': 1020,
                'stddev': 10
            },
            {
                'id_': '3',
                'client': 'john',
                'timestamp': UtcDateTime('2014-09-01T02:00:00Z'),
                'capture_name': 'capture #3',
                'count': 500,
                'min_': 10000,
                'max_': 20000,
                'average': 15000,
                'stddev': 500
            }
        ]
        self.generic_tests(captures, CaptureEvent)

    def test_support_table(self):
        pass

    def test_ui_table(self):
        pass


if __name__ == '__main__':
    unittest.main()