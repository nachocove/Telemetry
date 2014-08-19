import math


class Statistics:
    def __init__(self, count=0, min_=0.0, max_=0.0, first_moment=0.0, second_moment=0.0):
        if isinstance(count, Statistics):
            # copy constructor
            self.count = count.count
            self.min = count.min
            self.max = count.max
            self.first_moment = count.first_moment
            self.second_moment = count.second_moment
            return

        self.count = count
        if count == 0:
            self.min = None
            self.max = None
            self.first_moment = 0.0
            self.second_moment = 0.0
        else:
            self.min = float(min_)
            self.max = float(max_)
            self.first_moment = float(first_moment)
            self.second_moment = float(second_moment)

            # Sanity check the values
            mean = self.mean()
            if min_ > mean:
                raise ValueError('1st moment results in mean less than min.')
            if max_ < mean:
                raise ValueError('1st moment results in mean less than max.')
            if self.second_moment < (self.mean() ** 2):
                raise ValueError('2nd moment results in negative variance.')

    def __add__(self, other):
        """
        + operator combines two statistics objects
        """
        if self.count == 0 and other.count == 0:
            return Statistics(count=0)
        if self.count == 0:
            return Statistics(count=other.count,
                              min_=other.min,
                              max_=other.max,
                              first_moment=other.first_moment,
                              second_moment=other.second_moment)
        if other.count == 0:
            return Statistics(count=self.count,
                              min_=self.min,
                              max_=self.max,
                              first_moment=self.first_moment,
                              second_moment=self.second_moment)

        count = self.count + other.count
        max_ = max(self.max, other.max)
        min_ = min(self.min, other.min)

        first_moment = self.first_moment + other.first_moment
        second_moment = self.second_moment + other.second_moment

        return Statistics(count=count, min_=min_, max_=max_, first_moment=first_moment, second_moment=second_moment)

    def add_sample(self, sample):
        self.count += 1
        self.first_moment += sample
        self.second_moment += sample ** 2
        if self.min is None:
            self.min = sample
        else:
            if self.min > sample:
                self.min = sample
        if self.max is None:
            self.max = sample
        else:
            if self.max < sample:
                self.max = sample

    def mean(self):
        return self.first_moment / float(self.count)

    def variance(self):
        return self.second_moment - (self.mean() ** 2)

    def stddev(self):
        return math.sqrt(self.variance())