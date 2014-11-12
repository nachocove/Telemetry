from AWS.query import Query
from monitor_base import Monitor
from misc.number_formatter import pretty_number


class MonitorCount(Monitor):
    def __init__(self, conn, desc, rate_desc=None, start=None, end=None):
        Monitor.__init__(self, conn, desc, start, end)
        self.count = 0
        self.rate_desc = rate_desc

        # Create the query
        self.query = Query()
        self.query.add_range('uploaded_at', start, end)
        self.query.count = True

    def run(self):
        # Derived class must provide its own implementation
        raise Exception('must override')

    def report(self, summary):
        rate = Monitor.compute_rate(self.count, self.start, self.end, 'hr')
        count_str = pretty_number(self.count)
        self.logger.info('%s: %s', self.desc, count_str)
        self.logger.info('%s: %s', self.rate_desc, rate)
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
        self.logger.info('Querying %s...', self.desc)
        self.count = Query.users(self.query, self.conn)


class MonitorEvents(MonitorCount):
    def __init__(self, conn, start=None, end=None):
        MonitorCount.__init__(self, conn, 'Event count', 'Event rate', start, end)

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        self.count = Query.events(self.query, self.conn)
