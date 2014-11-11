import unittest
from boto.dynamodb2.layer1 import DynamoDBConnection
from events import *
from selectors import *
from tables import *
from query import Query
from query_filter import QueryFilter


def sublist(list_, indexes):
    return [list_[x] for x in indexes]


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
                                 'timestamp__lte': 500
                             })

    def test_greaterthanequal(self):
        self.query_filter.add('timestamp', SelectorGreaterThanEqual(600))
        self.assertDictEqual(self.query_filter.data(),
                             {
                                 'timestamp__gte': 600
                             })


class TestTables(unittest.TestCase):
    """
    Verify extensively many variations of LogTable. Only do spot checking for all
    other tables.
    """
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
        # Make sure the set of keys are the same. Note that we need to strip the trailing '_'
        # in the expected values
        expected_keys = sorted([x.rstrip('_') for x in expected.keys()])
        got_keys = sorted(got.keys())
        self.assertListEqual(expected_keys, got_keys)

        # Make sure the values are the same
        for (field, value) in expected.items():
            field = field.rstrip('_')  # remove trailing '_' (used to avoid python keyword conflicts)
            self.assertEqual(got[field], value, field)

    def compare_events_list(self, expected, got):
        self.assertEqual(len(expected), len(got))
        for n in range(len(expected)):
            self.compare_events(expected[n], got[n])

    def compare_events_one(self, expected, got):
        self.assertEqual(1, len(got))
        self.compare_events(expected, got[0])

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
                'timestamp': UtcDateTime('2014-11-01T07:00:00Z'),
                'uploaded_at': UtcDateTime('2014-11-01T07:00:00.001Z'),
                'event_type': 'INFO',
                'message': 'info log #1'
            },
            {
                'id_': '2',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-11-01T08:00:00Z'),
                'uploaded_at': UtcDateTime('2014-11-01T08:00:00.001Z'),
                'event_type': 'INFO',
                'message': 'info log #2'
            },
            {
                'id_': '3',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-11-01T08:00:05Z'),
                'uploaded_at': UtcDateTime('2014-11-01T08:00:05.001Z'),
                'event_type': 'WARN',
                'message': 'warn log #3'
            },
            {
                'id_': '4',
                'client': 'john',
                'timestamp': UtcDateTime('2014-11-01T08:00:10Z'),
                'uploaded_at': UtcDateTime('2014-11-01T08:00:10.001Z'),
                'event_type': 'ERROR',
                'message': 'error log #4'
            },
            {
                'id_': '5',
                'client': 'john',
                'timestamp': UtcDateTime('2014-11-01T09:00:11Z'),
                'uploaded_at': UtcDateTime('2014-11-01T09:00:11.001Z'),
                'event_type': 'INFO',
                'message': 'info log #5'
            },
            {
                'id_': '6',
                'client': 'john',
                'timestamp': UtcDateTime('2014-11-01T10:00:12Z'),
                'uploaded_at': UtcDateTime('2014-11-01T10:00:12.001Z'),
                'event_type': 'DEBUG',
                'message': 'debug log #6'
            }
        ]
        self.generic_tests(logs, LogEvent)

        # Query by client id
        query = Query()
        query.add('client', SelectorEqual('bob'))
        events = Query.events(query, self.connection)
        self.compare_events_list(logs[0:3], events)

        query = Query()
        query.add('client', SelectorEqual('john'))
        events = Query.events(query, self.connection)
        self.compare_events_list(logs[3:6], events)

        # Query by client id + timestamp
        query = Query()
        query.add('client', SelectorEqual('bob'))
        query.add('timestamp', SelectorEqual(UtcDateTime('2014-11-01T08:00:05Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(logs[2], events)

        # Query by client id + timestamp range
        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorLessThan(UtcDateTime('2014-11-01T09:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_list(logs[3:5], events)

        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorGreaterThan(UtcDateTime('2014-11-01T08:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_list(logs[4:6], events)

        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorGreaterThanEqual(UtcDateTime('2014-11-01T08:30:00Z')))
        query.add('timestamp', SelectorLessThan(UtcDateTime('2014-11-01T09:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(logs[4], events)

        # Query by event_type
        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        events = Query.events(query, self.connection)
        self.compare_events_list(sublist(logs, [0, 1, 4]), events)

        query = Query()
        query.add('event_type', SelectorEqual('ERROR'))
        events = Query.events(query, self.connection)
        self.compare_events_one(logs[3], events)

        query = Query()
        query.add('event_type', SelectorEqual('WARN'))
        events = Query.events(query, self.connection)
        self.compare_events_one(logs[2], events)

        query = Query()
        query.add('event_type', SelectorEqual('DEBUG'))
        events = Query.events(query, self.connection)
        self.compare_events_one(logs[5], events)

        # Query by event_type + timestamp
        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        query.add('timestamp', SelectorEqual(UtcDateTime('2014-11-01T09:00:11Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(logs[4], events)

        # Query by event_type + timestamp range
        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        query.add('timestamp', SelectorLessThan(UtcDateTime('2014-11-01T08:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_list(logs[:2], events)

        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        query.add('timestamp', SelectorGreaterThanEqual(UtcDateTime('2014-11-01T07:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_list(sublist(logs, [1, 4]), events)

        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        query.add_range('timestamp', UtcDateTime('2014-11-01T07:30:00Z'), UtcDateTime('2014-11-01T08:30:00Z'))
        events = Query.events(query, self.connection)
        self.compare_events_one(logs[1], events)

        # Query by timestamp range (should fall back to a scan)
        query = Query()
        #query.add_range('timestamp', UtcDateTime('2014-11-01T07:30:00Z'), UtcDateTime('2014-11-01T08:30:00Z'))
        query.add_range('timestamp', UtcDateTime('2014-11-01T07:30:00Z'), UtcDateTime('2014-11-01T08:30:00Z'))
        events = Query.events(query, self.connection)
        self.compare_events_list(logs[1:4], events)

    def test_wbxml_table(self):
        pass

    def test_counter_table(self):
        # Create items
        counters = [
            {
                'id_': '1',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-10-17T01:00:00Z'),
                'uploaded_at': UtcDateTime('2014-10-17T01:00:00.001Z'),
                'counter_name': 'counter B',
                'count': 101,
                'counter_start': UtcDateTime('2014-10-17T01:00:00Z'),
                'counter_end': UtcDateTime('2014-10-17T01:10:00Z')
            },
            {
                'id_': '2',
                'client': 'john',
                'timestamp': UtcDateTime('2014-10-17T02:00:00Z'),
                'uploaded_at': UtcDateTime('2014-10-17T02:00:00.001Z'),
                'counter_name': u'counter A',
                'count': 1001,
                'counter_start': UtcDateTime('2014-10-17T02:00:00Z'),
                'counter_end': UtcDateTime('2014-10-17T02:01:00Z')
            },
            {
                'id_': '3',
                'client': 'john',
                'timestamp': UtcDateTime('2014-10-17T03:00:00Z'),
                'uploaded_at': UtcDateTime('2014-10-17T03:00:00.001Z'),
                'counter_name': 'counter B',
                'count': 201,
                'counter_start': UtcDateTime('2014-10-17T03:00:00Z'),
                'counter_end': UtcDateTime('2014-10-17T03:01:00Z')
            }
        ]
        self.generic_tests(counters, CounterEvent)

        # Query by client id
        query = Query()
        query.add('client', SelectorEqual('john'))
        events = Query.events(query, self.connection)
        self.compare_events_list(counters[1:3], events)

        # Query by client id + timestamp
        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorEqual(UtcDateTime('2014-10-17T03:00:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(counters[2], events)

        # Query by counter name
        query = Query()
        query.add('counter_name', SelectorEqual('counter B'))
        events = Query.events(query, self.connection)
        self.compare_events_list(sublist(counters, [0, 2]), events)

        # Query by counter name + timestamp
        query = Query()
        query.add('counter_name', SelectorEqual('counter B'))
        query.add('timestamp', SelectorGreaterThanEqual(UtcDateTime('2014-10-17T03:00:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(counters[2], events)

    def test_capture_table(self):
        # Create items
        captures = [
            {
                'id_': '1',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-09-01T00:00:00Z'),
                'uploaded_at': UtcDateTime('2014-09-01T00:00:00.001Z'),
                'capture_name': 'capture A',
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
                'uploaded_at': UtcDateTime('2014-09-01T01:00:00.001Z'),
                'capture_name': 'capture B',
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
                'uploaded_at': UtcDateTime('2014-09-01T02:00:00.001Z'),
                'capture_name': 'capture A',
                'count': 500,
                'min_': 10000,
                'max_': 20000,
                'average': 15000,
                'stddev': 500
            }
        ]
        self.generic_tests(captures, CaptureEvent)

        # Query by client id
        query = Query()
        query.add('client', SelectorEqual('john'))
        events = Query.events(query, self.connection)
        self.compare_events_list(captures[1:3], events)

        # Query by client id + timestamp
        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorEqual(UtcDateTime('2014-09-01T02:00:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(captures[2], events)

        # Query by capture name
        query = Query()
        query.add('capture_name', SelectorEqual('capture A'))
        events = Query.events(query, self.connection)
        self.compare_events_list(sublist(captures, [0, 2]), events)

        # Query by capture name + timestamp
        query = Query()
        query.add('capture_name', SelectorEqual('capture A'))
        query.add('timestamp', SelectorLessThanEqual(UtcDateTime('2014-09-01T00:00:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(captures[0], events)

    def test_support_table(self):
        supports = [
            {
                'id_': '1',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-10-01T07:00:00Z'),
                'uploaded_at': UtcDateTime('2014-10-01T07:00:00.001Z'),
                'support': '{"email": "bob@company.com"}'
            },
            {
                'id_': '2',
                'client': 'john',
                'timestamp': UtcDateTime('2014-10-01T07:01:00Z'),
                'uploaded_at': UtcDateTime('2014-10-01T07:01:00.001Z'),
                'support': '{"email": "john@company.com"}'
            }
        ]
        self.generic_tests(supports, SupportEvent)

    def test_ui_table(self):
        ui_events = [
            {
                'id_': '1',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-10-01T07:05:00.001Z'),
                'uploaded_at': UtcDateTime('2014-10-01T07:05:00.002Z'),
                'ui_type': 'UIViewController',
                'ui_object': 'AttachmentViewController',
                'ui_string': 'will appear begin'
            },
            {
                'id_': '2',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-10-01T07:05:00.052Z'),
                'uploaded_at': UtcDateTime('2014-10-01T07:05:00.053Z'),
                'ui_type': 'UIViewController',
                'ui_object': 'AttachmentViewController',
                'ui_string': 'will appear end'
            },
            {
                'id_': '3',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-10-01T07:05:10.113Z'),
                'uploaded_at': UtcDateTime('2014-10-01T07:05:10.114Z'),
                'ui_type': 'UISegmentedControl',
                'ui_object': 'By file, date, contact',
                'ui_integer': 2
            },
            {
                'id_': '4',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-10-01T07:05:20.204Z'),
                'uploaded_at': UtcDateTime('2014-10-01T07:05:20.205Z'),
                'ui_type': 'UIButton',
                'ui_object': 'Dismiss',
            },
            {
                'id_': '5',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-10-01T07:05:20.255Z'),
                'uploaded_at': UtcDateTime('2014-10-01T07:05:20.256Z'),
                'ui_type': 'UIViewController',
                'ui_object': 'AttachmentViewController',
                'ui_string': 'will disappear begin'
            },
            {
                'id_': '6',
                'client': 'bob',
                'timestamp': UtcDateTime('2014-10-01T07:05:20.255Z'),
                'uploaded_at': UtcDateTime('2014-10-01T07:05:20.256Z'),
                'ui_type': 'UIViewController',
                'ui_object': 'AttachmentViewController',
                'ui_string': 'will disappear end'
            },
        ]
        self.generic_tests(ui_events, UiEvent)


if __name__ == '__main__':
    unittest.main()