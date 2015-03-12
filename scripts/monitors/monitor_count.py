import json
import time
from decimal import Decimal
from AWS.query import Query
from AWS.tables import TABLE_CLASSES
from AWS.selectors import SelectorEqual
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

class MonitorUserDataUsage(MonitorCount):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'Active User Data Usage')
        MonitorCount.__init__(self, *args, **kwargs)
        self.query.count=False
        self.query.attributes = ('client',)

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        self.users = set([x['client'] for x in Query.users(self.query, self.conn)])
        self.count = len(self.users)
        self.logger.debug("Found %d unique client id's", self.count)
        self.client_ids = dict()
        for client_id in self.users:
            self.client_ids[client_id] = object()
            query = Query()
            query.add('client', SelectorEqual(client_id))
            query.add_range('timestamp', self.start, self.end)
            query.count = False
            #query.attributes = ['id', 'client', 'event_type', 'message', 'wbxml', 'support']
            self.client_ids[client_id] = self.ClientIdLogInfo(self.conn, Query.events(query, self.conn))

    IN_S3 = ('INFO', 'WBXML')
    IN_DYNAMODB = ('WARN', 'ERROR', 'CAPTURE', 'COUNTER', 'UI', 'SUPPORT')
    S3_PRICING = 0.0300 # $ per GB

    class ClientIdLogInfo(object):
        def __init__(self, conn, log_row_query):
            self.log_rows = [x for x in log_row_query]
            self.log_row_count = len(self.log_rows)
            self.log_size = 0
            self.breakdown = {}
            for l in self.log_rows:
                event_type = l['event_type']
                if event_type.startswith('WBXML'):
                    event_type = 'WBXML'
                if event_type not in self.breakdown:
                    self.breakdown[event_type] = {'Rows': 0, 'Size': 0}
                self.breakdown[event_type]['Rows'] += 1
                size = len(json.dumps(l._data, default=str))
                self.breakdown[event_type]['Size'] += size
                self.log_size += size

    def k_string(self, num):
        return "{:6.2f}k".format(float(num)/1000.0)

    def m_string(self, num):
        return "{:6.2f}M".format(float(num)/1024.0/1024.0)

    def report(self, summary, **kwargs):
        count_str = pretty_number(self.count)
        self.logger.info('%s: %s', self.desc, count_str)
        summary.add_entry(self.desc, count_str)
        if self.count == 0:
            return

        table = Table()
        row = TableRow([TableHeader(Bold('Clients')),
                        TableHeader(Bold('Number of log-rows')),
                        TableHeader(Bold('Size of logs')),
                        TableHeader(Bold('Breakdown DyDB')),
                        TableHeader(Bold('Breakdown S3')),
                        ])
        table.add_row(row)
        total_rows = 0
        dydb_size = 0
        s3_size = 0
        number_of_samples = 0

        for client_id in self.client_ids:
            total_rows += self.client_ids[client_id].log_row_count
            number_of_samples += 1
            breakdown_dydb_str = ""
            breakdown_s3_str = ""
            s3_sum = 0
            dydb_sum = 0
            for event_type in self.client_ids[client_id].breakdown:
                event_size = self.client_ids[client_id].breakdown[event_type]['Size']
                breakdown_str = "%s Rows %s Size %s (avg size %s)\n" % (event_type,
                                                          self.k_string(self.client_ids[client_id].breakdown[event_type]['Rows']),
                                                          self.m_string(event_size),
                                                          event_size/self.client_ids[client_id].breakdown[event_type]['Rows'])
                if event_type in self.IN_DYNAMODB:
                    dydb_sum += event_size
                    dydb_size += event_size
                    breakdown_dydb_str += breakdown_str
                elif event_type in self.IN_S3:
                    s3_sum += event_size
                    s3_size += event_size
                    breakdown_s3_str += breakdown_str
                else:
                    raise Exception("Unhandled event-type %s" % event_type)

            if breakdown_dydb_str:
                breakdown_dydb_str += "\nSum(size)=%s" % self.m_string(dydb_sum)
            if breakdown_s3_str:
                breakdown_s3_str += "\nSum(size)=%s" % self.m_string(s3_sum)

            row = TableRow([TableElement(Text(client_id)),
                            TableElement(Text(self.k_string(self.client_ids[client_id].log_row_count))),
                            TableElement(Text(self.m_string(self.client_ids[client_id].log_size))),
                            TableElement(Text(breakdown_dydb_str, keep_linefeed=True)),
                            TableElement(Text(breakdown_s3_str, keep_linefeed=True)),
                            ])
            table.add_row(row)

        summary.add_entry("Number of telemetry.log rows", "{:,}".format(total_rows))
        summary.add_entry("Average DyDb MB/client", "{:6.2f}M".format(float(dydb_size)/1024.0/1024.0/float(self.count)))
        summary.add_entry("Event-type in DyDb", ", ".join(self.IN_DYNAMODB))
        summary.add_entry("Average S3 MB/client", "{:6.2f}M".format(float(s3_size)/1024.0/1024.0/float(self.count)))
        summary.add_entry("Event-type in S3", ", ".join(self.IN_S3))

        row = TableRow([TableElement(Text(Bold("Per Client Averages"))),
                        TableElement(Text("{:6.2f}k".format(float(total_rows)/1000.0/float(self.count)))),
                        TableElement(Text("")),
                        TableElement(Text("{:6.2f}M".format(float(dydb_size)/1024.0/1024.0/float(self.count)))),
                        TableElement(Text("{:6.2f}M".format(float(s3_size)/1024.0/1024.0/float(self.count)))),
                        ])
        table.add_row(row)
        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

class MonitorEmails(MonitorCount):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'New user count (fresh install)')
        kwargs.setdefault('rate_desc', 'New user rate')
        MonitorCount.__init__(self, *args, **kwargs)
        self.query.count = False
        self.active_clients_this_period = dict()
        self.new_clients_this_period = dict()
        self.new_emails_per_domain = dict()
        self.active_emails_per_domain = dict()
        self.debug_timing = False
        self.new_count = 0
        self.active_count = 0

    def _get_active_clients_this_period(self):
        clients = dict()
        query = Query()
        query.add_range('timestamp', self.start, self.end)
        t1 = time.time()
        results = Query.users(query, self.conn)
        t2 = time.time()
        if self.debug_timing: self.logger.debug("TIME active_clients_this_period %s: results %d %s", (t2-t1), len(results), query)
        for x in results:
            if x['client'] not in clients or clients[x['client']]['timestamp'] < x['timestamp']:
                clients[x['client']] = x
        return clients

    def _get_support_events_for_clients(self, clients):
        assert isinstance(clients, list)
        results = []
        for client_id in clients:
            query = Query()
            query.add('event_type', SelectorEqual('SUPPORT'))
            query.add('client', SelectorEqual(client_id))
            query.add_range('timestamp', self.start, self.end)
            t1 = time.time()
            r = Query.events(query, self.conn)
            t2 = time.time()
            if self.debug_timing: self.logger.debug("TIME %s: results %d %s", (t2-t1), len(r), query)
            if r:
                results.extend(r)
        return results

    def _summarize_emails_addresses(self, events):
        email_addresses = dict()
        for event in events:
            try:
                email = json.loads(event.get('support', '{}')).get('sha256_email_address', '')
                if email:
                    if email not in email_addresses or email_addresses[email]['timestamp'] < event['timestamp']:
                        email_addresses[email] = event
            except ValueError:
                # bad json
                continue
        return email_addresses

    def _summarize_emails_per_domain(self, emails):
        emails_per_domain = dict()
        for email in emails.keys():
            userhash, domain = email.split('@')
            if domain not in emails_per_domain:
                emails_per_domain[domain] = dict()
            emails_per_domain[domain][email] = emails[email]
        return emails_per_domain

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        # find all users that ran the client (i.e. inserted a device_info row) in the given timeframe
        # NOTE This is looking at timestamp (when the log was created) and NOT when it was uploaded
        self.active_clients_this_period = self._get_active_clients_this_period()
        self.new_clients_this_period = {x: y for x,y in self.active_clients_this_period.iteritems() if y['fresh_install'] == Decimal(1)}

        # Using the client and timestamp range, see which of the active users ran auto-d at all
        results = self._get_support_events_for_clients(self.new_clients_this_period.keys())
        new_email_addresses = self._summarize_emails_addresses(results)
        self.new_emails_per_domain = self._summarize_emails_per_domain(new_email_addresses)
        self.new_count = len(new_email_addresses)
        self.logger.debug('Found %d new emails: %s', self.new_count, new_email_addresses.keys())

        results = self._get_support_events_for_clients(self.active_clients_this_period.keys())
        active_emails = self._summarize_emails_addresses(results)
        self.active_emails_per_domain = self._summarize_emails_per_domain(active_emails)
        self.active_count = len(active_emails)
        self.logger.debug('Found %d active emails: %s', self.active_count, active_emails.keys())

    def _report_emails(self, email_dict, title, subtitle):
        paragraph_elements = []
        paragraph_elements.append(Bold(title))
        email_domain_table = Table()
        email_domain_table.add_row(TableRow([TableHeader(Bold('Domain')),
                                             TableHeader(Bold('# clients')),
                                             ]))
        for domain in sorted(email_dict.keys()):
            email_domain_table.add_row(TableRow([TableElement(Text(domain)),
                                                 TableElement(Text(pretty_number(len(email_dict[domain]))), align='right'),
                                                 ]))
        paragraph_elements.append(email_domain_table)

        for domain in sorted(email_dict.keys()):
            per_domain_table = Table()
            per_domain_table.add_row(TableRow([TableHeader(Bold('Email')),
                                               TableHeader(Bold('Last Seen')),
                                               ]))
            for email in email_dict[domain]:
                per_domain_table.add_row(TableRow([TableElement(Text(email)),
                                                   TableElement(Text(str(email_dict[domain][email]['timestamp']))),
                                                   ]))
            paragraph_elements.append(Bold("%s for domain %s" % (subtitle, domain)))
            paragraph_elements.append(per_domain_table)
        return paragraph_elements

    def report(self, summary, **kwargs):
        summary.add_entry(self.desc, pretty_number(self.new_count))
        rate = Monitor.compute_rate(self.new_count, self.start, self.end, 'hr')
        if rate is not None:
            summary.add_entry("New user rate", rate)

        summary.add_entry("Active emails", pretty_number(self.active_count))
        rate = Monitor.compute_rate(self.active_count, self.start, self.end, 'hr')
        if rate is not None:
            summary.add_entry("Active emails rate", rate)

        paragraph_elements = []
        paragraph_elements.extend(self._report_emails(self.new_emails_per_domain, self.title(), "New Emails"))
        paragraph_elements.extend(self._report_emails(self.active_emails_per_domain, "Active Emails", "Active Emails"))
        paragraph = Paragraph(paragraph_elements)
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
