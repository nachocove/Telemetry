from argparse import ArgumentParser
import Parse
import pprint


def query_log(log_type, conn):
    query = Parse.query.Query()
    query.add('event_type', Parse.query.SelectorEqual(log_type))
    query.limit = 1000
    return Parse.query.Query.objects('Events', query, conn)[0]


def classify_log(logs):
    report = dict()
    for log in logs:
        if log['message'] not in report:
            report[log['message']] = 1
        else:
            report[log['message']] += 1
    return report


def process_errors(conn):
    print 'Querying errors...'
    errors = query_log('ERROR', conn)
    print '%d errors found' % len(errors)
    return classify_log(errors)


def process_warnings(conn):
    print 'Querying warnings...'
    warnings = query_log('WARN', conn)
    print '%d warnings found' % len(warnings)
    return classify_log(warnings)


def print_report(report):
    new_report = dict()
    for (message, count) in report.items():
        if len(message) > 70:
            new_message = message[:76] + ' ...'
        else:
            new_message = message
        new_report[new_message] = report[message]
    pprint.PrettyPrinter(depth=4).pprint(new_report)


def main():
    parser = ArgumentParser()
    parser.add_argument('--api-key',
                        help='REST API key [default: NachoMail API key]',
                        required=True)
    parser.add_argument('--app-id',
                        help='application ID',
                        required=True)
    parser.add_argument('--session-token',
                        help='session token',
                        default=None)

    parser.add_argument('--errors',
                        help='process error logs',
                        action='store_true',
                        default=False)
    parser.add_argument('--warnings',
                        help='process warning logs',
                        action='store_true',
                        default=False)

    options = parser.parse_args()

    # Create a connection
    conn = Parse.connection.Connection(app_id=options.app_id,
                                       api_key=options.api_key,
                                       session_token=options.session_token)

    # Do queries
    errors = None
    warnings = None
    if options.errors:
        errors = process_errors(conn)
    if options.warnings:
        warnings = process_warnings(conn)

    # Generate report
    if errors is not None:
        print_report(errors)
    if warnings is not None:
        print_report(warnings)

if __name__ == '__main__':
    main()
