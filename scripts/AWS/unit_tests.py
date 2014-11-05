import unittest

from query_filter import *


class TestQueryFilter(unittest.TestCase):
    def setUp(self):
        self.query_filter = QueryFilter()

    def test_equal(self):
        self.query_filter.add('event_type', SelectorEqual('INFO'))
        self.assertDictEqual(self.query_filter.data(),
            {
                'event_type_EQ': 'INFO'
            })

    def test_notequal(self):
        self.query_filter.add('event_type', SelectorNotEqual('ERROR'))
        self.assertDictEqual(self.query_filter.data(),
            {
                'event_type_NE': 'ERROR'
            })

    def test_lessthan(self):
        self.query_filter.add('timestamp', SelectorLessThan(123))
        self.assertDictEqual(self.query_filter.data(),
            {
                'timestamp_LT': 123
            })

    def test_greaterthan(self):
        self.query_filter.add('timestamp', SelectorGreaterThan(400))
        self.assertDictEqual(self.query_filter.data(),
            {
                'timestamp_GT': 400
            })

    def test_lessthanequal(self):
        self.query_filter.add('timestamp', SelectorLessThanEqual(500))
        self.assertDictEqual(self.query_filter.data(),
            {
                'timestamp_LE': 500
            })

    def test_greaterthanequal(self):
        self.query_filter.add('timestamp', SelectorGreaterThanEqual(600))
        self.assertDictEqual(self.query_filter.data(),
            {
                'timestamp_GE': 600
            })

if __name__ == '__main__':
    unittest.main()