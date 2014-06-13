import Parse
from monitor_base import Monitor
from number_formatter import pretty_number


class MonitorCount(Monitor):
    def __init__(self, conn, desc, rate_desc=None, start=None, end=None):
        Monitor.__init__(self, conn, desc, start, end)
        self.count = 0
        self.rate_desc = rate_desc

        # Create the query
        self.query = Parse.query.Query()
        if self.start is not None:
            self.query.add('createdAt', Parse.query.SelectorGreaterThanEqual(start))
        if self.end is not None:
            self.query.add('createdAt', Parse.query.SelectorLessThan(end))
        self.query.limit = 0
        self.query.count = 1

    def run(self):
        # Derived class must provide its own implementation
        raise Exception('must override')

    def report(self, summary):
        rate = Monitor.compute_rate(self.count, self.start, self.end, 'hr')
        count_str = pretty_number(self.count)
        print '%s: %s' % (self.desc, count_str)
        print '%s: %s' % (self.rate_desc, rate)
        summary.add_entry(self.desc, count_str)

        if self.rate_desc and rate is not None:
            summary.add_entry(self.rate_desc, rate)
        return None  # does not have its own report

    def attachment(self):
        return None


class MonitorUsers(MonitorCount):
    def __init__(self, conn, start=None, end=None):
        MonitorCount.__init__(self, conn, 'New user count', 'New user rate', start, end)

    def run(self):
        print 'Querying %s...' % self.desc
        self.count = Parse.query.Query.users(self.query, self.conn)[1]


class MonitorEvents(MonitorCount):
    def __init__(self, conn, start=None, end=None):
        MonitorCount.__init__(self, conn, 'Event count', 'Event rate', start, end)

    def run(self):
        print 'Querying %s...' % self.desc
        self.count = Parse.query.Query.objects('Events', self.query, self.conn)[1]
