import unittest
from misc.number_formatter import *


class TestNumberFormatter(unittest.TestCase):
    def test_valid_number_str(self):
        self.assertTrue(valid_number_str('123'))
        self.assertTrue(valid_number_str('123.45'))
        self.assertTrue(valid_number_str('1234.0'))
        self.assertTrue(valid_number_str('0.1234'))
        self.assertTrue(valid_number_str('0.12340'))
        self.assertTrue(valid_number_str('1'))
        self.assertTrue(valid_number_str('0.5'))

        self.assertFalse(valid_number_str('abc'))  # not a number
        self.assertFalse(valid_number_str('01234'))  # extra leading 0
        self.assertFalse(valid_number_str('00.1234'))  # extra leading 0
        self.assertFalse(valid_number_str('0.12.45'))  # two decimal places
        self.assertFalse(valid_number_str('1234x56'))  # invalid decimal place

    def test_commafy(self):
        self.assertEqual(commafy('1'), '1')
        self.assertEqual(commafy('12'), '12')
        self.assertEqual(commafy('123'), '123')
        self.assertEqual(commafy('1234'), '1,234')
        self.assertEqual(commafy('12345'), '12,345')
        self.assertEqual(commafy('123456'), '123,456')
        self.assertEqual(commafy('1234567'), '1,234,567')
        self.assertEqual(commafy('12345678'), '12,345,678')
        self.assertEqual(commafy('123456789'), '123,456,789')
        self.assertEqual(commafy('1234567890'), '1,234,567,890')

        self.assertEqual(commafy('1.99'), '1.99')
        self.assertEqual(commafy('12.99'), '12.99')
        self.assertEqual(commafy('123.99'), '123.99')
        self.assertEqual(commafy('1234.99'), '1,234.99')
        self.assertEqual(commafy('12345.99'), '12,345.99')
        self.assertEqual(commafy('123456.99'), '123,456.99')
        self.assertEqual(commafy('1234567.99'), '1,234,567.99')
        self.assertEqual(commafy('12345678.99'), '12,345,678.99')
        self.assertEqual(commafy('123456789.99'), '123,456,789.99')
        self.assertEqual(commafy('1234567890.99'), '1,234,567,890.99')

    def test_pretty_number(self):
        self.assertEqual(pretty_number(123), '123')
        self.assertEqual(pretty_number(12.0), '12.0')
        self.assertEqual(pretty_number(123456.0), '123,456.0')
        self.assertEqual(pretty_number(123.456), '123.456')
        self.assertEqual(pretty_number(12345678.901), '12,345,678.90')
        self.assertEqual(pretty_number(12.3456789), '12.3456')

if __name__ == '__main__':
    unittest.main()