import os

import unittest
from boto.dynamodb2.layer1 import DynamoDBConnection
import time

from AWS.events import *
from AWS.selectors import *
from AWS.tables import *
from AWS.query import Query
from AWS.query_filter import QueryFilter


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

def dynamo_server(dynamo_home, port):
    args = ['%s/bin/java' % os.environ.get('JAVA_HOME', '/usr'), '-Djava.library.path=%s/DynamoDBLocal_lib' % dynamo_home,
           '-jar', '%s/DynamoDBLocal.jar' % dynamo_home,
           '--port', str(port)]
    os.execv(args[0], args)

DynamoLocalProcess = None
def start_dynamo(port):
    global DynamoLocalProcess
    if not DynamoLocalProcess and 'DYNAMODBLOCAL_HOME' in os.environ:
        from multiprocessing import Process
        p = Process(target=dynamo_server, args=(os.environ['DYNAMODBLOCAL_HOME'], port))
        p.start()
        time.sleep(2)
        print "Started process %d" % p.pid
        DynamoLocalProcess = p
    else:
        raise Exception('Could not start the local dynamoDB server. Perhaps env["DYNAMODBLOCAL_HOME"] is not set?')

def kill_dynamo():
    global DynamoLocalProcess
    if DynamoLocalProcess:
        DynamoLocalProcess.terminate()  # kill the process
        DynamoLocalProcess.join()  # reap the process (not sure this is strictly necessary)
        DynamoLocalProcess = None

class DynamoLocalUnitTest(unittest.TestCase):
    DYNAMO_LOCAL_PORT = 8000
    DynamoLocalProcess = None

    @classmethod
    def setUpClass(cls):
        #if os.path.exists('unittest_localhost.db'):
        #    os.remove('unittest_localhost.db')

        start_dynamo(port=cls.DYNAMO_LOCAL_PORT)

    @classmethod
    def tearDownClass(cls):
        kill_dynamo()

    def setUp(self):
        self.connection = DynamoDBConnection(host='localhost',
                                             port=self.DYNAMO_LOCAL_PORT,
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


class TestTables(DynamoLocalUnitTest):
    """
    Generic class to set up and do rudimentary tests on a table.

    Sub-classes need to set event_cls and items.
    """

    event_cls = None
    items = None

    def setUp(self):
        super(TestTables, self).setUp()
        self.insert_items()

    def insert_items(self):
        # Insert them into db
        for item in self.items:
            event = self.event_cls(self.connection, **item)
            event.save()

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

    def generic_read_tests(self):
        """
        Scan all items. Query by id on all of them.
        :return:
        """
        # Scan and verify them
        events = self.event_cls.scan(self.connection)
        for event in events:
            print str(event) + '\n'
            index = int(event['id']) - 1
            self.compare_events(self.items[index], event)

        # Query by id
        for index in range(len(self.items)):
            id_ = index + 1
            query = Query()
            query.add('id', SelectorEqual(str(id_)))
            events = Query.events(query, self.connection)
            self.assertEqual(len(events), 1)
            self.compare_events(self.items[index], events[0])

class TestLogTable(TestTables):
    event_cls = LogEvent
    items = [
        {
            'id_': '1',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-11-01T07:00:00Z'),
            'uploaded_at': UtcDateTime('2014-11-01T07:00:00.001Z'),
            'event_type': 'INFO',
            'thread_id': '1',
            'message': 'info log #1'
        },
        {
            'id_': '2',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-11-01T08:00:00Z'),
            'uploaded_at': UtcDateTime('2014-11-01T08:00:00.001Z'),
            'event_type': 'INFO',
            'thread_id': '2',
            'message': 'info log #2'
        },
        {
            'id_': '3',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-11-01T08:00:05Z'),
            'uploaded_at': UtcDateTime('2014-11-01T08:00:05.001Z'),
            'event_type': 'WARN',
            'thread_id': '3',
            'message': 'warn log #3'
        },
        {
            'id_': '4',
            'client': 'john',
            'timestamp': UtcDateTime('2014-11-01T08:00:10Z'),
            'uploaded_at': UtcDateTime('2014-11-01T08:00:10.001Z'),
            'event_type': 'ERROR',
            'thread_id': '4',
            'message': 'error log #4'
        },
        {
            'id_': '5',
            'client': 'john',
            'timestamp': UtcDateTime('2014-11-01T09:00:11Z'),
            'uploaded_at': UtcDateTime('2014-11-01T09:00:11.001Z'),
            'event_type': 'INFO',
            'thread_id': '5',
            'message': 'info log #5'
        },
        {
            'id_': '6',
            'client': 'john',
            'timestamp': UtcDateTime('2014-11-01T10:00:12Z'),
            'uploaded_at': UtcDateTime('2014-11-01T10:00:12.001Z'),
            'event_type': 'DEBUG',
            'thread_id': '6',
            'message': 'debug log #6',
        }
    ]

    def test_log_table(self):
        self.generic_read_tests()

    def test_query_client(self):
        # Query by client id
        query = Query()
        query.add('client', SelectorEqual('bob'))
        events = Query.events(query, self.connection)
        self.compare_events_list(self.items[0:3], events)

        query = Query()
        query.add('client', SelectorEqual('john'))
        events = Query.events(query, self.connection)
        self.compare_events_list(self.items[3:6], events)

    def test_query_client_timestamp(self):
        # Query by client id + timestamp
        query = Query()
        query.add('client', SelectorEqual('bob'))
        query.add('timestamp', SelectorEqual(UtcDateTime('2014-11-01T08:00:05Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[2], events)

        # Query by client id + timestamp range
        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorLessThan(UtcDateTime('2014-11-01T09:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_list(self.items[3:5], events)

        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorGreaterThan(UtcDateTime('2014-11-01T08:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_list(self.items[4:6], events)

        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorGreaterThanEqual(UtcDateTime('2014-11-01T08:30:00Z')))
        query.add('timestamp', SelectorLessThan(UtcDateTime('2014-11-01T09:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[4], events)

    def test_query_event_type(self):
        # Query by event_type
        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        events = Query.events(query, self.connection)
        self.compare_events_list(sublist(self.items, [0, 1, 4]), events)

        query = Query()
        query.add('event_type', SelectorEqual('ERROR'))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[3], events)

        query = Query()
        query.add('event_type', SelectorEqual('WARN'))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[2], events)

        query = Query()
        query.add('event_type', SelectorEqual('DEBUG'))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[5], events)

    @unittest.skip('fails locally')
    def test_query_event_type_timestamp(self):
        # Query by event_type + timestamp
        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        query.add('timestamp', SelectorEqual(UtcDateTime('2014-11-01T09:00:11Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[4], events)

        # Query by event_type + timestamp range
        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        query.add('timestamp', SelectorLessThan(UtcDateTime('2014-11-01T08:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_list(self.items[:2], events)

        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        query.add('timestamp', SelectorGreaterThanEqual(UtcDateTime('2014-11-01T07:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_list(sublist(self.items, [1, 4]), events)

        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        query.add_range('timestamp', UtcDateTime('2014-11-01T07:30:00Z'), UtcDateTime('2014-11-01T08:30:00Z'))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[1], events)

    def test_query_timestamp(self):
        # Query by timestamp range (should fall back to a scan)
        query = Query()
        query.add_range('timestamp', UtcDateTime('2014-11-01T07:30:00Z'), UtcDateTime('2014-11-01T08:30:00Z'))
        events = Query.events(query, self.connection)
        self.compare_events_list(self.items[1:4], events)

    def test_query_event_type_uploaded_at(self):
        # Query by event_type and uploaded_at range
        query = Query()
        query.add('event_type', SelectorEqual('INFO'))
        query.add('uploaded_at', SelectorGreaterThanEqual(UtcDateTime('2014-11-01T07:30:00Z')))
        query.add('uploaded_at', SelectorLessThan(UtcDateTime('2014-11-01T08:30:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[1], events)

        query = Query()
        query.add_range('uploaded_at', UtcDateTime('2014-11-01T07:30:00Z'), UtcDateTime('2014-11-01T08:30:00Z'))
        events = Query.events(query, self.connection)
        self.compare_events_list(self.items[1:4], events)

class TestWBXMLTable(TestTables):
    event_cls = WbxmlEvent
    items = [
        {
            'id_': '1',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-17T01:00:00Z'),
            'uploaded_at': UtcDateTime('2014-10-17T01:00:00.001Z'),
            'event_type': 'WBXML_REQUEST',
            'wbxml': "WHATGOESHERE?",
        },
        {
            'id_': '2',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-17T01:00:00Z'),
            'uploaded_at': UtcDateTime('2014-10-17T01:00:00.001Z'),
            'event_type': 'WBXML_RESPONSE',
            'wbxml': "WHATGOESHERE?",
        },
    ]

    def test_wbxml_table(self):
        self.generic_read_tests()

class TestCounterTable(TestTables):
    event_cls = CounterEvent
    items = [
        {
            'id_': '1',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-17T01:00:00Z'),
            'uploaded_at': UtcDateTime('2014-10-17T01:00:00.001Z'),
            'counter_name': 'counter B',
            'count': 101,
            'counter_start': UtcDateTime('2014-10-17T01:00:00Z'),
            'counter_end': UtcDateTime('2014-10-17T01:10:00Z'),
            'event_type': 'COUNTER',
        },
        {
            'id_': '2',
            'client': 'john',
            'timestamp': UtcDateTime('2014-10-17T02:00:00Z'),
            'uploaded_at': UtcDateTime('2014-10-17T02:00:00.001Z'),
            'counter_name': u'counter A',
            'count': 1001,
            'counter_start': UtcDateTime('2014-10-17T02:00:00Z'),
            'counter_end': UtcDateTime('2014-10-17T02:01:00Z'),
            'event_type': 'COUNTER',
        },
        {
            'id_': '3',
            'client': 'john',
            'timestamp': UtcDateTime('2014-10-17T03:00:00Z'),
            'uploaded_at': UtcDateTime('2014-10-17T03:00:00.001Z'),
            'counter_name': 'counter B',
            'count': 201,
            'counter_start': UtcDateTime('2014-10-17T03:00:00Z'),
            'counter_end': UtcDateTime('2014-10-17T03:01:00Z'),
            'event_type': 'COUNTER',
        }
    ]

    def test_counter_table(self):
        # Create items
        self.generic_read_tests()

    def test_query_client(self):
        # Query by client id
        query = Query()
        query.add('client', SelectorEqual('john'))
        events = Query.events(query, self.connection)
        self.compare_events_list(self.items[1:3], events)

        # Query by client id + timestamp
        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorEqual(UtcDateTime('2014-10-17T03:00:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[2], events)

        # Query by counter name
        query = Query()
        query.add('counter_name', SelectorEqual('counter B'))
        events = Query.events(query, self.connection)
        self.compare_events_list(sublist(self.items, [0, 2]), events)

        # Query by counter name + timestamp
        query = Query()
        query.add('counter_name', SelectorEqual('counter B'))
        query.add('timestamp', SelectorGreaterThanEqual(UtcDateTime('2014-10-17T03:00:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[2], events)

        # Query by event_type + uploaded_at
        query = Query()
        query.add('event_type', SelectorEqual('COUNTER'))
        query.add_range('uploaded_at', UtcDateTime('2014-10-17T01:30:00Z'), UtcDateTime('2014-10-17T02:30:00Z'))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[1], events)

        # Count by event_type + uploaded_at
        query.count = True
        self.assertEqual(1, Query.events(query, self.connection))

class TestCaptureTable(TestTables):
    event_cls = CaptureEvent
    items = [
        {
            'id_': '1',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-09-01T00:00:00Z'),
            'uploaded_at': UtcDateTime('2014-09-01T00:00:00.001Z'),
            'capture_name': 'capture A',
            'count': 100,
            'min_': 100,
            'max_': 200,
            'sum_': 15000,
            'sum2': 2500000,
            'event_type': 'CAPTURE',
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
            'sum_': 51000,
            'sum2': 52025000,
            'event_type': 'CAPTURE',
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
            'sum_': 7500000,
            'sum2': 112625000000,
            'event_type': 'CAPTURE',
        }
    ]

    def test_capture_table(self):
        # Create items
        self.generic_read_tests()

        # Query by client id
        query = Query()
        query.add('client', SelectorEqual('john'))
        events = Query.events(query, self.connection)
        self.compare_events_list(self.items[1:3], events)

        # Query by client id + timestamp
        query = Query()
        query.add('client', SelectorEqual('john'))
        query.add('timestamp', SelectorEqual(UtcDateTime('2014-09-01T02:00:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[2], events)

        # Query by capture name
        query = Query()
        query.add('capture_name', SelectorEqual('capture A'))
        events = Query.events(query, self.connection)
        self.compare_events_list(sublist(self.items, [0, 2]), events)

        # Query by capture name + timestamp
        query = Query()
        query.add('capture_name', SelectorEqual('capture A'))
        query.add('timestamp', SelectorLessThanEqual(UtcDateTime('2014-09-01T00:00:00Z')))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[0], events)

        # Query by event_type + uploaded_at
        query = Query()
        query.add('event_type', SelectorEqual('CAPTURE'))
        query.add_range('uploaded_at', UtcDateTime('2014-09-01T00:30:00Z'), UtcDateTime('2014-09-01T01:30:00Z'))
        events = Query.events(query, self.connection)
        self.compare_events_one(self.items[1], events)

        # Count by event_type + uploaded_at
        query.count = True
        self.assertEqual(1, Query.events(query, self.connection))

class TestSupportTable(TestTables):
    event_cls = SupportEvent
    items = [
        {
            'id_': '1',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-01T07:00:00Z'),
            'uploaded_at': UtcDateTime('2014-10-01T07:00:00.001Z'),
            'support': '{"email": "bob@company.com"}',
            'event_type': 'SUPPORT',
        },
        {
            'id_': '2',
            'client': 'john',
            'timestamp': UtcDateTime('2014-10-01T07:01:00Z'),
            'uploaded_at': UtcDateTime('2014-10-01T07:01:00.001Z'),
            'support': '{"email": "john@company.com"}',
            'event_type': 'SUPPORT',
        }
    ]
    def test_support_table(self):
        self.generic_read_tests()

class TestUITable(TestTables):
    event_cls = UiEvent
    items = [
        {
            'id_': '1',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-01T07:05:00.001Z'),
            'uploaded_at': UtcDateTime('2014-10-01T07:05:00.002Z'),
            'ui_type': 'UIViewController',
            'ui_object': 'AttachmentViewController',
            'ui_string': 'will appear begin',
            'event_type': 'UI',
        },
        {
            'id_': '2',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-01T07:05:00.052Z'),
            'uploaded_at': UtcDateTime('2014-10-01T07:05:00.053Z'),
            'ui_type': 'UIViewController',
            'ui_object': 'AttachmentViewController',
            'ui_string': 'will appear end',
            'event_type': 'UI',
        },
        {
            'id_': '3',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-01T07:05:10.113Z'),
            'uploaded_at': UtcDateTime('2014-10-01T07:05:10.114Z'),
            'ui_type': 'UISegmentedControl',
            'ui_object': 'By file, date, contact',
            'ui_integer': 2,
            'event_type': 'UI',
        },
        {
            'id_': '4',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-01T07:05:20.204Z'),
            'uploaded_at': UtcDateTime('2014-10-01T07:05:20.205Z'),
            'ui_type': 'UIButton',
            'ui_object': 'Dismiss',
            'event_type': 'UI',
        },
        {
            'id_': '5',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-01T07:05:20.255Z'),
            'uploaded_at': UtcDateTime('2014-10-01T07:05:20.256Z'),
            'ui_type': 'UIViewController',
            'ui_object': 'AttachmentViewController',
            'ui_string': 'will disappear begin',
            'event_type': 'UI',
        },
        {
            'id_': '6',
            'client': 'bob',
            'timestamp': UtcDateTime('2014-10-01T07:05:20.255Z'),
            'uploaded_at': UtcDateTime('2014-10-01T07:05:20.256Z'),
            'ui_type': 'UIViewController',
            'ui_object': 'AttachmentViewController',
            'ui_string': 'will disappear end',
            'event_type': 'UI',
        },
    ]

    def test_ui_table(self):
        self.generic_read_tests()


if __name__ == '__main__':
    unittest.main()