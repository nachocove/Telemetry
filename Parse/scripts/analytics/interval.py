class Interval:
    def __init__(self, start=None, stop=None):
        self.start = start
        self.stop = stop

    def is_complete(self):
        return self.start is not None and self.stop is not None

    def value(self):
        # TODO - There is a bug in client telemetry code that results in some events
        # in the wrong order. For now, we'll reject these events by raising an
        # exception here and catching it above
        if self.start > self.stop:
            raise ValueError
        delta = self.stop - self.start
        return (float(delta.days) * 86400.0) + float(delta.seconds) + (float(delta.microseconds) / 1.e6)
