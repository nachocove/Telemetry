import json
import time
from AWS.query import Query
from AWS.tables import TABLE_CLASSES
from AWS.selectors import SelectorEqual, SelectorContains, SelectorStartsWith
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
        self.active_clients_this_period = set()
        self.clients_that_did_autod = set()
        self.email_addresses = set()
        self.emails_per_domain = dict()

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        # find all users that ran the client (i.e. inserted a device_info row) in the given timeframe
        # NOTE This is looking at timestamp (when the log was created) and NOT when it was uploaded
        query = Query()
        query.add_range('timestamp', self.start, self.end)
        t1 = time.time()
        results = Query.users(query, self.conn)
        t2 = time.time()
        self.logger.debug("TIME %s: %s", (t2-t1), query)
        for x in results:
            self.active_clients_this_period.add(x['client'])

        # Using the client and timestamp range, see which of the active users ran auto-d at all
        results = []
        for clientID in self.active_clients_this_period:
            query = Query()
            query.add_range('timestamp', self.start, self.end)
            query.add('client', SelectorEqual(clientID))
            query.add('message', SelectorStartsWith('AUTOD'))
            t1 = time.time()
            results.extend(Query.events(query, self.conn))
            t2 = time.time()
            self.logger.debug("TIME %s: %s", (t2-t1), query)

        for event in results:
            self.clients_that_did_autod.add(event['client'])
        results = []
        for clientID in self.clients_that_did_autod:
            query = Query()
            query.add('event_type', SelectorEqual('SUPPORT'))
            query.add('client', SelectorEqual(clientID))
            query.add_range('timestamp', self.start, self.end)
            t1 = time.time()
            results.extend(Query.events(query, self.conn))
            t2 = time.time()
            self.logger.debug("TIME %s: %s", (t2-t1), query)
        for event in results:
            try:
                email = json.loads(event.get('support', '{}')).get('sha256_email_address', '')
                if email:
                    self.email_addresses.add(email)
            except ValueError:
                # bad json
                continue
        self.count = len(self.email_addresses)
        self.logger.debug('Found %d emails: %s', self.count, self.email_addresses)
        for email in self.email_addresses:
            userhash, domain = email.split('@')
            if not domain in self.emails_per_domain:
                self.emails_per_domain[domain] = []
            self.emails_per_domain[domain].append(email)

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
