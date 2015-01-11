import unittest
from misc.event_formatter import LogStyleEventFormatter, RecordStyleEventFormatter


class TestLogStyleFormatter(unittest.TestCase):
    def test_format_info(self):
        obj = {'timestamp': {'__type': 'Date',
                             'iso': '2014-05-24T12:34:56.789Z'},
               'event_type': 'INFO',
               'client': 'abcxyz',
               'build_version': '1.0.0',
               'os_type': 'iPhone OS',
               'os_version': '7.1.1',
               'device_model': 'iPhone 5,2',
               'message': 'This is an INFO log.'}
        output = LogStyleEventFormatter(prefix="alpha").format(obj)
        expected_output = "2014-05-24T12:34:56.789Z INFO [abcxyz, 1.0.0, iPhone OS, 7.1.1, iPhone 5,2] http://localhost:8000/bugfix/alpha/logs/abcxyz/2014-05-24T12:34:56.789Z/1/ This is an INFO log. "
        self.assertEqual(output, expected_output)

    def test_format_warn_no_ident(self):
        obj = {'timestamp': {'__type': 'Date',
                             'iso': '2014-05-24T12:34:56.789Z'},
               'event_type': 'WARN',
               'message': 'This is a WARN log.'}
        output = LogStyleEventFormatter(prefix="alpha").format(obj)
        expected_output = '2014-05-24T12:34:56.789Z WARN  This is a WARN log. '
        self.assertEqual(output, expected_output)


class TestRecordStyleFormatter(unittest.TestCase):
    def test_format_info(self):
        obj = {'timestamp': {'__type': 'Date',
                             'iso': '2014-05-24T12:34:56.789Z'},
               'event_type': 'INFO',
               'client': 'abcxyz',
               'build_version': '1.0.0',
               'os_type': 'iPhone OS',
               'os_version': '7.1.1',
               'device_model': 'iPhone 5,2',
               'message': 'This is an INFO log.'}
        output = RecordStyleEventFormatter(prefix="alpha").format(obj)
        expected_output = '''timestamp: 2014-05-24T12:34:56.789Z
event_type: INFO
client: abcxyz
build_version: 1.0.0
os_type: iPhone OS
os_version: 7.1.1
device_model: iPhone 5,2
telemetry: http://localhost:8000/bugfix/alpha/logs/abcxyz/2014-05-24T12:34:56.789Z/1/
message: This is an INFO log.
'''
        self.assertEqual(output, expected_output)

    def test_format_warn_no_ident(self):
        obj = {'timestamp': {'__type': 'Date',
                             'iso': '2014-05-24T12:34:56.789Z'},
               'event_type': 'WARN',
               'message': 'This is a WARN log.'}
        output = RecordStyleEventFormatter(prefix="alpha").format(obj)
        self.assertEqual(output, 'timestamp: 2014-05-24T12:34:56.789Z\n'
                                 'event_type: WARN\n'
                                 'message: This is a WARN log.\n')

if __name__ == '__main__':
    unittest.main()