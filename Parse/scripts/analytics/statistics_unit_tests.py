import unittest
import math
from statistics import Statistics


class TestStatistics(unittest.TestCase):
    def setUp(self):
        self.stats1 = Statistics(count=0)
        # samples: 100, 200
        self.stats2 = Statistics(count=2, min_=100, max_=200, first_moment=300, second_moment=50000)
        # samples: 200, 300
        self.stats3 = Statistics(count=2, min_=200, max_=300, first_moment=500, second_moment=130000)
        # samples: 100, 300
        self.stats4 = Statistics(count=2, min_=100, max_=300, first_moment=400, second_moment=100000)

    def compare(self, stats, count, min_, max_, first, second):
        self.assertEqual(stats.count, count)
        self.assertEqual(stats.min, min_)
        self.assertEqual(stats.max, max_)
        self.assertEqual(stats.first_moment, first)
        self.assertEqual(stats.second_moment, second)

    def test_constructor(self):
        self.compare(self.stats1, 0, None, None, 0.0, 0.0)
        self.compare(self.stats2, 2, 100., 200., 300., 50000.)

        self.assertRaises(ValueError, Statistics, count=10, min_=200,
                          max_=100, first_moment=1500, second_moment=2500000)
        self.assertRaises(ValueError, Statistics, count=10, min_=100,
                          max_=200, first_moment=2500, second_moment=6500000)

    def test_add_sample(self):
        stats = Statistics()
        stats.add_sample(-10.0)
        self.compare(stats, 1, -10.0, -10.0, -10.0, 100.0)

        stats.add_sample(10.0)
        self.compare(stats, 2, -10.0, 10.0, 0.0, 200.0)

        self.assertEqual(stats.mean(), 0.0)
        self.assertEqual(stats.variance(), 100.0)
        self.assertEqual(stats.stddev(), 10.0)

        stats.add_sample(0.0)
        self.compare(stats, 3, -10.0, 10.0, 0.0, 200.0)

    def test_add_operator(self):
        sum1 = self.stats2 + self.stats3
        self.compare(sum1, 4, 100.0, 300.0, 800.0, 180000.0)

        sum2 = sum1 + self.stats4
        self.compare(sum2, 6, 100.0, 300.0, 1200.0, 280000.0)

if __name__ == '__main__':
    unittest.main()