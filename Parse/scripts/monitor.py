import argparse
import Parse
import pprint
import config
import os
import ConfigParser
import emails
import getpass
from utc_datetime import UtcDateTime
from html_elements import *


def query_log(log_type, conn, start, end):
    query = Parse.query.Query()
    query.add('event_type', Parse.query.SelectorEqual(log_type))
    if start is not None:
        query.add('createdAt', Parse.query.SelectorGreaterThanEqual(start))
    if end is not None:
        query.add('createdAt', Parse.query.SelectorLessThan(end))
    query.limit = 1000
    query.skip = 0
    obj_list = []

    # Keep querying until the list is less than 1000
    results = Parse.query.Query.objects('Events', query, conn)[0]
    obj_list.extend(results)
    while len(results) == query.limit:
        query.skip += query.limit
        results = Parse.query.Query.objects('Events', query, conn)[0]
        obj_list.extend(results)
    return obj_list


def classify_log(logs):
    report = dict()
    for log in logs:
        if log['message'] not in report:
            report[log['message']] = 1
        else:
            report[log['message']] += 1
    return report


def process_messages(conn, event_type, desc, start, end):
    print 'Querying %s...' % desc
    events = query_log(event_type, conn, start, end)
    print '%d %s found' % (len(events), desc)
    report = classify_log(events)
    return process_report(report), len(events)


def process_errors(conn, start, end):
    print 'Querying errors...'
    errors = query_log('ERROR', conn, start, end)
    print '%d errors found' % len(errors)
    report = classify_log(errors)
    return process_report(report), len(errors)


def process_warnings(conn, start, end):
    print 'Querying warnings...'
    warnings = query_log('WARN', conn, start, end)
    print '%d warnings found' % len(warnings)
    report = classify_log(warnings)
    return process_report(report), len(warnings)


def process_report(report):
    new_report = dict()
    for (message, count) in report.items():
        if len(message) > 70:
            new_message = message[:76] + ' ...'
        else:
            new_message = message
        new_report[new_message] = report[message]
    return new_report


def output_report(report, title, email):
    pprint.PrettyPrinter(depth=4).pprint(report)
    if email:
        table = Table()
        row = TableRow([TableHeader(Bold('Count')),
                        TableHeader(Bold('Message'))])
        table.add_row(row)
        for (message, count) in report.items():
            row = TableRow([TableElement(Text(str(count))),
                            TableElement(Text(message))])
            table.add_row(row)
        paragraph = Paragraph([Bold(title), table])
        email.content.add(paragraph)


def read_timestamp(cfg_file):
    if not os.path.exists(cfg_file):
        return None
    cfg = ConfigParser.RawConfigParser()
    cfg.read(cfg_file)
    if cfg.has_section('timestamps') and cfg.has_option('timestamps', 'last'):
        return UtcDateTime(cfg.get('timestamps', 'last'))
    return None


def write_timestamp(cfg_file, utc_now):
    cfg = ConfigParser.RawConfigParser()
    cfg.read(cfg_file)

    if not cfg.has_section('timestamps'):
        cfg.add_section('timestamps')
    cfg.set('timestamps', 'last', str(utc_now))
    with open(cfg_file, 'w') as f:
        cfg.write(f)


def read_email_settings(cfg_file):
    cfg = ConfigParser.RawConfigParser()
    cfg.read(cfg_file)

    email = emails.Email()

    try:
        server = cfg.get('email', 'smtp_server')
        port = int(cfg.getint('email', 'port'))
        username = cfg.get('email', 'username')
        if cfg.has_option('email', 'password'):
            password = cfg.get('email', 'password')
        else:
            password = getpass.getpass('Email password: ')
        if cfg.has_option('email', 'start_tls'):
            start_tls = cfg.getboolean('email', 'start_tls')
        else:
            start_tls = False
        if cfg.has_option('email', 'tls'):
            tls = cfg.getboolean('email', 'tls')
        else:
            tls = False

        email.from_address = username
        email.to_addresses = [cfg.get('email', 'recipient')]

        smtp_server = emails.EmailServer(server=server, port=port, username=username,
                                         password=password,tls=tls, start_tls=start_tls)
        return smtp_server, email
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        return None, None


class DateTimeAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        if (option_string != '--start') and (option_string != '--end'):
            raise ValueError('unexpected option %s with datetime ' % option_string)
        if (option_string == '--start') and ('last' == value):
            setattr(namespace, self.dest, 'last')
        elif (option_string == '--end') and ('now' == value):
            setattr(namespace, self.dest, 'now')
        else:
            setattr(namespace, self.dest, UtcDateTime(value))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key',
                        help='REST API key [default: NachoMail API key]',
                        default=None)
    parser.add_argument('--app-id',
                        help='application ID',
                        default=None)
    parser.add_argument('--session-token',
                        help='session token',
                        default=None)

    parser.add_argument('--config',
                        help='configuration file',
                        default='monitor.cfg')
    parser.add_argument('--start',
                        help='time window starting time in ISO-8601 UTC or "last" for the last saved time',
                        action=DateTimeAction,
                        default=None)
    parser.add_argument('--end',
                        help='time window ending time in ISO-8601 UTC or "now" for the current time',
                        action=DateTimeAction,
                        default=None)
    parser.add_argument('--email',
                        help='send email notification',
                        action='store_true',
                        default=False)

    parser.add_argument('--errors',
                        help='process error logs',
                        action='store_true',
                        default=False)
    parser.add_argument('--warnings',
                        help='process warning logs',
                        action='store_true',
                        default=False)

    options = parser.parse_args()

    # If no key is provided in command line, get them from config.
    if not options.api_key or not options.app_id or not options.session_token:
        config.read_config(options)
    else:
        config.write_config(options)

    # If we want a time window but do not have one from command line, get it
    # from config and current time
    do_update_timestamp = False
    if options.start == 'last':
        options.start = read_timestamp(options.config)
    if options.end == 'now':
        options.end = UtcDateTime.now()
        do_update_timestamp = True
    print 'Start time: %s' % options.start
    print 'End time: %s' % options.end

    # If send email, we want to make sure that the email credential is there
    (smtp_server, email) = read_email_settings(options.config)
    if smtp_server is None:
        print 'ERROR: no email configuration'
        exit(1)
    email.content = Html()

    # Create a connection
    conn = Parse.connection.Connection(app_id=options.app_id,
                                       api_key=options.api_key,
                                       session_token=options.session_token)

    # Do queries
    errors_report = None
    warnings_report = None
    if options.errors:
        (errors_report, num_errors) = process_messages(conn=conn, event_type='ERROR', desc='errors',
                                                       start=options.start, end=options.end)
    if options.warnings:
        (warnings_report, num_warnings) = process_messages(conn=conn, event_type='WARN', desc='warnings',
                                                           start=options.start, end=options.end)


    # Create summary section of the email
    table = Table()

    def add_table(desc, value):
        table.add_row(TableRow([TableElement(Text(desc)),
                                TableElement(Text(str(value)))]))

    if options.start:
        start_time = str(options.start)
    else:
        start_time = ''
    if options.end:
        end_time = str(options.end)
    else:
        end_time = ''
    email.subject = 'Telemetry summary [%s]' % end_time
    add_table('Start time', start_time)
    add_table('End time', end_time)
    add_table('# of errors', num_errors)
    add_table('# of warnings', num_warnings)
    email.content.add(Paragraph([Bold('Summary\n'), table]))

    # Generate report
    if (errors_report is not None) and (len(errors_report) > 0):
        output_report(errors_report, 'Errors\n', email)
    if (warnings_report is not None) and (len(warnings_report) > 0):
        output_report(warnings_report, 'Warnings\n', email)

    # Send the email
    if options.email:
        print 'Sending email...'
        with open('monitor.html', 'w') as f:
            print >>f, email.content.html()
        with open('monitor.txt', 'w') as f:
            print >>f, email.content.plain_text()
        email.send(smtp_server)

    # Update timestamp in config if necessary
    if do_update_timestamp:
        write_timestamp(options.config, options.end)

if __name__ == '__main__':
    main()
