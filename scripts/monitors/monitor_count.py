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
