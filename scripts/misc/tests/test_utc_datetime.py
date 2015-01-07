import unittest
from misc.utc_datetime import UtcDateTime


class TestUtcDateTime(unittest.TestCase):
    def setUp(self):
        self.test_vectors = ['2014-06-05T01:02:03.004Z',
                             '2013-11-30T23:59:40.111Z']

    def test_decode(self):
        """
        Convert str to UtcDateTime object.
        """
        dt1 = UtcDateTime(self.test_vectors[0])
        self.assertEqual(dt1.datetime.year, 2014)
        self.assertEqual(dt1.datetime.month, 6)
        self.assertEqual(dt1.datetime.day, 5)
        self.assertEqual(dt1.datetime.hour, 1)
        self.assertEqual(dt1.datetime.minute, 2)
        self.assertEqual(dt1.datetime.second, 3)
        self.assertEqual(dt1.datetime.microsecond, 4000)

        dt2 = UtcDateTime(self.test_vectors[1])
        self.assertEqual(dt2.datetime.year, 2013)
        self.assertEqual(dt2.datetime.month, 11)
        self.assertEqual(dt2.datetime.day, 30)
        self.assertEqual(dt2.datetime.hour, 23)
        self.assertEqual(dt2.datetime.minute, 59)
        self.assertEqual(dt2.datetime.second, 40)
        self.assertEqual(dt2.datetime.microsecond, 111000)

    def test_encode(self):
        """
        Convert UtcDateTime object to str.
        """
        for dt_str in self.test_vectors:
            dt = UtcDateTime(dt_str)
            self.assertEqual(str(dt), dt_str)

    def test_sub(self):
        """
        Test __sub__ works. There are a lot of tricky case involed in leap years.
        I'm going to ignore those as the elepase time in most case should be no
        more than a few hours to a day.
        """
        start = UtcDateTime('2014-06-05T01:02:04.001Z')
        end1 = UtcDateTime('2014-06-05T01:02:04.999Z')
        self.assertEqual(end1 - start, 0.998)

        end2 = UtcDateTime('2014-06-05T01:02:05.001Z')
        self.assertEqual(end2 - start, 1.0)

        end3 = UtcDateTime('2014-06-05T01:03:04.001Z')
        self.assertEqual(end3 - start, 60.0)

        end4 = UtcDateTime('2014-06-05T02:02:04.001Z')
        self.assertEqual(end4 - start, 3600.0)

        end5 = UtcDateTime('2014-06-06T01:02:04.001Z')
        self.assertEqual(end5 - start, 86400.0)

        end6 = UtcDateTime('2014-07-05T01:02:04.001Z')
        self.assertEqual(end6 - start, 86400.0 * 30)

        end7 = UtcDateTime('2015-06-05T01:02:04.001Z')
        self.assertEqual(end7 - start, 86400.0 * 365)

        end8 = UtcDateTime('2015-07-06T02:03:05.999Z')
        total = (86400.0 * (365 + 30 + 1)) + 3600.0 + 60.0 + 1.0 + 0.998
        self.assertEqual(end8 - start, total)
        self.assertEqual(start - end8, -total)

    def test_cmp(self):
        dt1a = UtcDateTime('2014-06-15T01:02:03.004Z')
        dt1b = UtcDateTime('2014-06-15T01:02:03.004Z')
        dt2 = UtcDateTime('2014-05-15T01:02:03.004Z')
        dt3 = UtcDateTime('2014-06-15T02:02:03.004Z')

        self.assertEqual(dt1a, dt1b)
        self.assertGreater(dt1a, dt2)
        self.assertLess(dt1a, dt3)

    def test_ticks(self):
        dt = UtcDateTime('2014-10-11T01:02:03.004Z')
        self.assertEqual(dt, UtcDateTime(dt.toticks()))


if __name__ == '__main__':
    unittest.main()