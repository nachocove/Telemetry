from AWS.query import Query
from AWS.tables import TABLE_CLASSES
from AWS.selectors import SelectorEqual
from misc.emails import emails_per_domain
from misc.html_elements import Table, TableRow, TableHeader, Bold, TableElement, Text, Paragraph
from monitor_base import Monitor
from misc.number_formatter import pretty_number


class MonitorCount(Monitor):
    def __init__(self, rate_desc=None, *args, **kwargs):
        Monitor.__init__(self, *args, **kwargs)
        self.count = 0
        self.rate_desc = rate_desc

        # Create the query
        self.query = Query()
        self.query.add_range('uploaded_at', self.start, self.end)
        self.query.count = True

    def run(self):
        # Derived class must provide its own implementation
        raise Exception('must override')

    def report(self, summary, **kwargs):
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
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'Active user count')
        MonitorCount.__init__(self, *args, **kwargs)

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        self.count = Query.users(self.query, self.conn)

    def report(self, summary, **kwargs):
        count_str = pretty_number(self.count)
        self.logger.info('%s: %s', self.desc, count_str)
        summary.add_entry(self.desc, count_str)
        return None  # does not have its own report

class MonitorEmails(MonitorCount):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'New user count (fresh install)')
        kwargs.setdefault('rate_desc', 'New user rate')
        MonitorCount.__init__(self, *args, **kwargs)
        self.query.count = False
        self.email_addresses = set()
        self.emails_per_domain = dict()
        self.debug_timing = False

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        # find all users that ran the client (i.e. inserted a device_info row) in the given timeframe
        # NOTE This is looking at timestamp (when the log was created) and NOT when it was uploaded
        self.email_addresses = Query.emails_per_domain(self.start, self.end, self.conn)
        self.count = len(self.email_addresses)
        self.logger.debug('Found %d emails: %s', self.count, self.email_addresses)
        self.emails_per_domain = emails_per_domain(self.email_addresses)

    def report(self, summary, **kwargs):
        rate = Monitor.compute_rate(self.count, self.start, self.end, 'hr')
        count_str = pretty_number(self.count)
        self.logger.info('%s: %s', self.desc, count_str)
        self.logger.info('%s: %s', self.rate_desc, rate)
        summary.add_entry(self.desc, count_str)

        if self.rate_desc and rate is not None:
            summary.add_entry(self.rate_desc, rate)
        table = Table()
        table.add_row(TableRow([TableHeader(Bold('Domain')),
                                TableHeader(Bold('# clients')),
                                ]))
        for domain in sorted(self.emails_per_domain.keys()):
            table.add_row(TableRow([TableElement(Text(domain)),
                                    TableElement(Text(pretty_number(len(self.emails_per_domain[domain]))), align='right'),
                                    ]))

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

    def attachment(self):
        return None




class MonitorEvents(MonitorCount):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'Event count')
        kwargs.setdefault('rate_desc', 'Event rate')
        MonitorCount.__init__(self, *args, **kwargs)

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        # We cannot just issue a query with a range on uploaded_at because it
        # will result in a scan. Instead, we iterate of all event types, issue
        # a query for event_type + uploaded_at range which results in a indexed query
        # for each event type and finally combine the count
        self.count = 0
        for table in TABLE_CLASSES:
            for event_type in table.EVENT_TYPES:
                query = Query()
                query.add('event_type', SelectorEqual(event_type))
                query.add_range('uploaded_at', self.start, self.end)
                query.count = True
                count = Query.events(query, self.conn)
                self.count += count
