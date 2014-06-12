import math


class Statistics:
    def __init__(self, count=0, min_=0.0, max_=0.0, average=0.0, stddev=0.0):
        if isinstance(count, Statistics):
            # copy constructor
            self.count = count.count
            self.min = count.min
            self.max = count.max
            self.average = count.average
            self.stddev = count.stddev
            return

        self.count = count
        if count == 0:
            self.min = None
            self.max = None
            self.average = None
            self.stddev = None
        else:
            # Check the values
            if not min_ <= average <= max_:
                raise ValueError('min must be <= average and average must be <= max')
            self.min = float(min_)
            self.max = float(max_)
            self.average = float(average)
            self.stddev = float(stddev)

    def first_moment(self):
        if self.count == 0:
            return 0.0
        return float(self.count) * self.average

    def second_moment(self):
        if self.count == 0:
            return 0.0
        if self.stddev is None:
            return None
        return ((self.stddev * self.stddev) + (self.average * self.average)) * self.count

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
                              average=other.average,
                              stddev=other.stddev)
        if other.count == 0:
            return Statistics(count=self.count,
                              min_=self.min,
                              max_=self.max,
                              average=self.average,
                              stddev=self.average)
        count = self.count + other.count
        max_ = max(self.max, other.max)
        min_ = min(self.min, other.min)

        total = self.first_moment() + other.first_moment()
        average = total / count

        total2 = self.second_moment() + other.second_moment()
        stddev = math.sqrt(total2/count - (average * average))

        return Statistics(count=count, min_=min_, max_=max_, average=average, stddev=stddev)