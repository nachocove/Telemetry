import argparse
import Parse
import pprint
import config
import ConfigParser
import emails
import getpass
from html_elements import *


class Summary(Table):
    def add_entry(self, desc, value):
        row = TableRow([TableElement(Text(desc)),
                        TableElement(Text(str(value)))])
        self.add_row(row)


class Monitor:
    def __init__(self, conn, desc, start=None, end=None):
        self.conn = conn
        self.desc = desc
        self.start = start
        self.end = end

    def run(self):
        raise Exception('not implemented')

    def report(self, summary):
        raise Exception('not implemented')

    def title(self):
        return self.desc[0].upper() + self.desc[1:]

    @staticmethod
    def compute_rate(count, start, end, unit):
        if start is None or end is None:
            return None
        if unit is 'sec':
            scale = 1.0
        elif unit is 'min':
            scale = 60.0
        elif unit is 'hr':
            scale = 3600.0
        else:
            raise ValueError('unit must be sec, min, or hr')
        return str(float(count) / (end - start) * scale) + ' / ' + unit


class MonitorLog(Monitor):
    def __init__(self, conn, event_type, desc, msg, rate_msg, start=None, end=None):
        Monitor.__init__(self, conn, desc, start, end)
        self.event_type = event_type
        self.events = list()  # events returned from the query
        self.report_ = dict()  # an analysis structure derived from raw events
        self.msg = msg  # message about total # of events in summary
        self.rate_msg = rate_msg  # message about the rate of the events in summary

    def _query(self):
        query = Parse.query.Query()
        query.add('event_type', Parse.query.SelectorEqual(self.event_type))
        if self.start is not None:
            query.add('createdAt', Parse.query.SelectorGreaterThanEqual(self.start))
        if self.end is not None:
            query.add('createdAt', Parse.query.SelectorLessThan(self.end))
        query.limit = 1000
        query.skip = 0

        # Keep querying until the list is less than 1000
        results = Parse.query.Query.objects('Events', query, self.conn)[0]
        self.events.extend(results)
        while len(results) == query.limit:
            query.skip += query.limit
            results = Parse.query.Query.objects('Events', query, self.conn)[0]
            self.events.extend(results)

    def _classify(self):
        self.report_ = dict()
        for log in self.events:
            if log['message'] not in self.report_:
                self.report_[log['message']] = 1
            else:
                self.report_[log['message']] += 1

    def run(self):
        print 'Querying %s...' % self.desc
        self._query()
        self._classify()

    @staticmethod
    def _process_report(report):
        new_report = dict()
        for (message, count) in report.items():
            if len(message) > 70:
                new_message = message[:76] + ' ...'
            else:
                new_message = message
            new_report[new_message] = report[message]
        return new_report

    def report(self, summary):
        count = len(self.events)
        rate = Monitor.compute_rate(count, self.start, self.end, 'hr')

        # Generate summary info
        print '%s: %d' % (self.msg, count)
        print '%s: %s' % (self.rate_msg, rate)
        summary.add_entry(self.msg, count)
        summary.add_entry(self.rate_msg, rate)

        # Create the monitor specific report if there is anything to report
        if count == 0:
            return None
        report = MonitorLog._process_report(self.report_)
        report_list = sorted(report.items(), key=lambda x: x[1], reverse=True)
        pprint.PrettyPrinter(depth=4).pprint(report_list)

        # Create paragraph with a header and table
        table = Table()
        row = TableRow([TableHeader(Bold('Count')),
                        TableHeader(Bold('Message'))])
        table.add_row(row)
        for (message, count) in report_list:
            row = TableRow([TableElement(Text(str(count))),
                            TableElement(Text(message))])
            table.add_row(row)

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph


class MonitorErrors(MonitorLog):
    def __init__(self, conn, start=None, end=None):
        MonitorLog.__init__(self, conn, event_type='ERROR', desc='errors',
                            msg='Error count', rate_msg='Error rate',
                            start=start, end=end)


class MonitorWarnings(MonitorLog):
    def __init__(self, conn, start=None, end=None):
        MonitorLog.__init__(self, conn, event_type='WARN', desc='warnings',
                            msg='Warning count', rate_msg='Warning rate',
                            start=start, end=end)


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
        print '%s: %d' % (self.desc, self.count)
        print '%s: %s' % (self.rate_desc, rate)
        summary.add_entry(self.desc, str(self.count))

        if self.rate_desc and self.end is not None and self.start is not None:
            rate = float(self.count) / (self.end - self.start) * 60
            summary.add_entry(self.rate_desc, str(rate) + '/ min')
        return None  # does not have its own report


class MonitorUsers(MonitorCount):
    def __init__(self, conn, start=None, end=None):
        MonitorCount.__init__(self, conn, 'New user count', 'New user rate', start, end)

    def run(self):
        self.count = Parse.query.Query.users(self.query, self.conn)[1]


class MonitorEvents(MonitorCount):
    def __init__(self, conn, start=None, end=None):
        MonitorCount.__init__(self, conn, 'Event count', 'Event rate', start, end)

    def run(self):
        self.count = Parse.query.Query.objects('Events', self.query, self.conn)[1]


class MonitorConfig(config.Config):
    def __init__(self, cfg_file):
        config.Config.__init__(self, cfg_file)

    def read_timestamp(self):
        try:
            timestamp = self.config.get('timestamps', 'last')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            return None
        return Parse.utc_datetime.UtcDateTime(timestamp)

    def write_timestamp(self, utc_now):
        if not self.config.has_section('timestamps'):
            self.config.add_section('timestamps')
        self.config.set('timestamps', 'last', str(utc_now))
        self.write()

    def read_email_settings(self):
        email = emails.Email()
        try:
            server = self.config.get('email', 'smtp_server')
            port = self.config.getint('email', 'port')
            username = self.config.get('email', 'username')
            if self.config.has_option('email', 'password'):
                password = self.config.get('email', 'password')
            else:
                password = getpass.getpass('Email password: ')
            if self.config.has_option('email', 'start_tls'):
                start_tls = self.config.getboolean('email', 'start_tls')
            else:
                start_tls = False
            if self.config.has_option('email', 'tls'):
                tls = self.config.getboolean('email', 'tls')
            else:
                tls = False

            email.from_address = username
            email.to_addresses = [self.config.get('email', 'recipient')]

            smtp_server = emails.EmailServer(server=server, port=port, username=username,
                                             password=password, tls=tls, start_tls=start_tls)
            return smtp_server, email
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            return None, None


class DateTimeAction(argparse.Action):
    """
    This class parses the ISO-8601 UTC time given in --after and --before.
    """
    def __call__(self, parser, namespace, value, option_string=None):
        if (option_string != '--after') and (option_string != '--before'):
            raise ValueError('unexpected option %s with datetime ' % option_string)
        if (option_string == '--after') and ('last' == value):
            setattr(namespace, self.dest, 'last')
        elif (option_string == '--before') and ('now' == value):
            setattr(namespace, self.dest, 'now')
        else:
            setattr(namespace, self.dest, Parse.utc_datetime.UtcDateTime(value))


def main():
    parser = argparse.ArgumentParser(add_help=False)
    config_group = parser.add_argument_group('Configuration Options',
                                             'These options specify the Parse credential and various '
                                             'configurations. You need app id, REST API key, and session '
                                             'token for user "monitor". To get the session token, use '
                                             'parse.py login command. A configuration file is created to '
                                             'store these parameters for you. So, you can omit these parameters '
                                             'after running with them once.')
    config_group.add_argument('--api-key',
                              help='REST API key [default: NachoMail API key]',
                              default=None)
    config_group.add_argument('--app-id',
                              help='Application ID',
                              default=None)
    config_group.add_argument('--session-token',
                              help='Session token',
                              default=None)
    config_group.add_argument('--config',
                              help='Configuration file (default: monitor.cfg)',
                              default='monitor.cfg')
    config_group.add_argument('--email',
                              help='Send email notification',
                              action='store_true',
                              default=False)

    filter_group = parser.add_argument_group('Filtering Options',
                                             'These options specify a time window where reports are applied.')
    filter_group.add_argument('--after',
                              help='Time window starting time in ISO-8601 UTC or "last" for the last saved time',
                              action=DateTimeAction,
                              dest='start',
                              default=None)
    filter_group.add_argument('--before',
                              help='Time window ending time in ISO-8601 UTC or "now" for the current time',
                              action=DateTimeAction,
                              dest='end',
                              default=None)

    misc_group = parser.add_argument_group('Miscellaneous Option')
    misc_group.add_argument('-h', '--help', help='Print this help message', action='store_true', dest='help')

    report_group = parser.add_argument_group('Monitors', 'These options select which report to run.')
    report_group.add_argument('monitors',
                              nargs='*',
                              metavar='MONITOR',
                              help='Choices are: users, events, errors, warnings')
    options = parser.parse_args()

    if options.help or len(options.monitors) == 0:
        parser.print_help()
        exit(0)

    # If no key is provided in command line, get them from config.
    config_ = MonitorConfig(options.config)
    if not options.api_key or not options.app_id or not options.session_token:
        config_.read_keys(options)
    else:
        config_.write_keys(options)

    # If we want a time window but do not have one from command line, get it
    # from config and current time
    do_update_timestamp = False
    if options.start == 'last':
        options.start = config_.read_timestamp()
    if options.end == 'now':
        options.end = Parse.utc_datetime.UtcDateTime.now()
        do_update_timestamp = True

    # If send email, we want to make sure that the email credential is there
    summary_table = Summary()
    if options.email:
        (smtp_server, email) = config_.read_email_settings()
        if smtp_server is None:
            print 'ERROR: no email configuration'
            exit(1)
        email.content = Html()
        email.content.add(Paragraph([Bold('Summary'), summary_table]))
    else:
        email = None
        smtp_server = None

    # Start processing
    print 'Start time: %s' % options.start
    print 'End time: %s' % options.end
    summary_table.add_entry('Start time', str(options.start))
    summary_table.add_entry('End time', str(options.end))

    # Create a connection
    conn = Parse.connection.Connection(app_id=options.app_id,
                                       api_key=options.api_key,
                                       session_token=options.session_token)

    # Run each monitor
    monitors = list()
    for monitor_name in options.monitors:
        mapping = {'errors': MonitorErrors,
                   'warnings': MonitorWarnings,
                   'users': MonitorUsers,
                   'events': MonitorEvents}
        if monitor_name not in mapping:
            print 'ERROR: unknown monitor %s. ignore' % monitor_name
            continue
        monitor = mapping[monitor_name](conn, options.start, options.end)
        monitors.append(monitor)
        monitor.run()

    # Generate all outputs
    for monitor in monitors:
        output = monitor.report(summary_table)
        if options.email and output is not None:
            email.content.add(output)

    # Send the email
    if options.email:
        print 'Sending email...'
        # Save the HTML and plain text body to files
        end_time = str(options.end)
        end_time_suffix = end_time.replace(':', '_').replace('-', '_').replace('.', '_')
        with open('monitor-email.%s.html' % end_time_suffix, 'w') as f:
            print >>f, email.content.html()
        with open('monitor-email.%s.txt' % end_time_suffix, 'w') as f:
            print >>f, email.content.plain_text()
        # Add title
        email.subject = 'Telemetry summary [%s]' % str(options.end)
        email.send(smtp_server)

    # Update timestamp in config if necessary after we have successfully
    # send the notification email
    if do_update_timestamp:
        config_.write_timestamp(options.end)

if __name__ == '__main__':
    main()
