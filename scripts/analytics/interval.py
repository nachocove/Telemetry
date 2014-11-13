class Interval:
    def __init__(self, start=None, stop=None):
        self.start = start
        self.stop = stop

    def is_complete(self):
        return self.start is not None and self.stop is not None

    def value(self):
        if self.start > self.stop:
            raise ValueError('invalid interval (start=%s, stop=%s)' % (self.start, self.stop))
        delta = self.stop - self.start
        return (float(delta.days) * 86400.0) + float(delta.seconds) + (float(delta.microseconds) / 1.e6)
