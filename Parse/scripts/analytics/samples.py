from statistics import Statistics


class Samples:
    """
    A sample is a statistically significant measurement. Various statistics
    can be derived from a set of samples.

    1. count - |X|
    2. mean - E[X]
    3. median - x s.t. Pr[X < x] = 0.5
    4. range - (min(X), max(X))
    5. variance (std. dev) - E[X^2] - E[X]^2
    """
    def __init__(self, description=None):
        self.description = description
        # By default, we avoid saving samples as it can become very memory intensive.
        self.__samples = None
        self.statistics = Statistics()

    def enable_median(self):
        assert self.statistics.count == 0  # must do this before any sample is added
        self.__samples = list()

    def enable_distribution(self):
        assert self.statistics.count == 0  # must do this before any sample is added
        self.__samples = list()

    def add(self, sample):
        """Add a sample to the set"""
        self.statistics.add_sample(sample)
        if self.__samples is not None:
            assert isinstance(self.__samples, list)
            # TODO - This is really inefficient and can lead to thrashing of memory allocator. Optimize it later
            self.__samples.append(sample)

    def median(self):
        if self.__samples is None:
            return None
        sorted_samples = sorted(self.__samples)
        midpoint = len(sorted_samples) / 2
        return sorted_samples[midpoint]

    def samples(self):
        if self.__samples is None:
            return None
        return self.__samples[:]  # make a copy

    def reset(self):
        if self.__samples is not None:
            self.__samples = list()
        self.statistics = Statistics()
