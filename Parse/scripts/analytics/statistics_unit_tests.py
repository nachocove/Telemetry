import unittest
import math
from statistics import Statistics


class TestStatistics(unittest.TestCase):
    def setUp(self):
        self.stats1 = Statistics(count=0)
        # samples: 100, 200
        self.stats2 = Statistics(count=2, min_=100, max_=200, average=150, stddev=50)
        # samples: 200, 300
        self.stats3 = Statistics(count=2, min_=200, max_=300, average=250, stddev=50)
        # samples: 100, 300
        self.stats4 = Statistics(count=2, min_=100, max_=300, average=200, stddev=100)

    def test_statistics(self):
        self.assertEqual(self.stats1.count, 0)
        self.assertEqual(self.stats1.min, None)
        self.assertEqual(self.stats1.max, None)
        self.assertEqual(self.stats1.average, None)
        self.assertEqual(self.stats1.stddev, None)

        self.assertEqual(self.stats2.count, 2)
        self.assertEqual(self.stats2.min, 100.)
        self.assertEqual(self.stats2.max, 200.)
        self.assertEqual(self.stats2.average, 150.)
        self.assertEqual(self.stats2.stddev, 50.)

        self.assertRaises(ValueError, Statistics, count=10, min_=200, max_=100, average=150, stddev=55)
        self.assertRaises(ValueError, Statistics, count=10, min_=100, max_=200, average=250, stddev=55)

    def test_add(self):
        sum1 = self.stats2 + self.stats3
        self.assertEqual(sum1.count, 4)
        self.assertEqual(sum1.min, 100.)
        self.assertEqual(sum1.max, 300.)
        self.assertEqual(sum1.average, 200.)
        self.assertEqual(sum1.stddev, math.sqrt(5000.0))

        sum2 = sum1 + self.stats4
        self.assertEqual(sum2.count, 6)
        self.assertEqual(sum2.min, 100.)
        self.assertEqual(sum2.max, 300.)
        self.assertEqual(sum2.average, 200.)
        self.assertLess(math.fabs(sum2.stddev - math.sqrt(20000./3.)), 0.00002)

if __name__ == '__main__':
    unittest.main()