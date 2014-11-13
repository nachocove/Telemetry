try:
    import dateutil
except ImportError:
    dateutil = None  # useless but makes PyCharm happy
    print ''
    print 'python-dateutil package is not found. Please make sure that it is installed.'
    print 'If you have easy_install, you can issue:'
    print ''
    print 'sudo easy_install python-dateutil'
    print ''
    exit(1)
import argparse
import getpass
import os
import pprint
import threading
import Queue
import time

import Parse
from misc import event_formatter
from misc import events
from misc import support
from misc import expression
from misc.utc_datetime import UtcDateTime
from misc.config import Config, ColorsConfig, WbxmlToolConfig


def abort(mesg):
    print 'ERROR: ' + mesg
    exit(1)


def get_query(options):
    if len(options.field) == 0 and len(options.selectors) == 0 and options.query:
        expr = expression.Expression.parse(options.query)
        query = expr.query()
    else:
        query = Parse.query.Query()
        assert len(options.field) == len(options.selectors)
        for n in range(len(options.field)):
            query.add(options.field[n], options.selectors[n])
    if options.limit is None:
        query.limit = 1000
    else:
        query.limit = options.limit
    query.keys = options.display

    return query


def login(options):
    """
    Log in a user and print the user info. Use to get the session token.
    """
    if not options.username:
        abort('username not specified')
    if not options.password:
        options.password = getpass.getpass('Password: ')

    conn = Parse.connection.Connection(app_id=options.app_id, api_key=options.api_key)
    user = Parse.users.User.login(username=options.username, password=options.password, conn=conn)
    if user:
        pp = pprint.PrettyPrinter(depth=4)
        pp.pprint(user.desc())


class ThreadSafeDeleteCounter:
    """
    This class provides a thread-safe counter
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.count = 0

    def click(self, id_, thread_id):
        self.lock.acquire()
        self.count += 1
        print '[%d:%s] Deleting object %s...' % (self.count, thread_id, id_)
        self.lock.release()


class DeleteThread(threading.Thread):
    def __init__(self, counter, options, desc):
        threading.Thread.__init__(self)
        self.conn = Parse.connection.Connection(app_id=options.app_id, api_key=None,
                                                master_key=options.master_key)
        self.counter = counter
        self.desc = desc
        self.obj_queue = Queue.Queue()

    def run(self):
        while True:
            obj = self.obj_queue.get()
            if obj is None:
                return  # None indicates the thread should terminate
            self.counter.click(obj.id, self.desc)
            obj.delete(self.conn)
            self.obj_queue.task_done()


def delete(options):
    """
    Delete all objects that match the query.
    """
    if not options.master_key:
        abort('master key is required for deletion')

    # This connection for querying
    conn = Parse.connection.Connection(app_id=options.app_id, api_key=None, master_key=options.master_key)

    # Set up query
    query = get_query(options)
    query.keys = []  # We only want the objectId

    # Set up multiple threads for deleting
    delete_counter = ThreadSafeDeleteCounter()
    num_threads = 4
    threads = [None] * num_threads
    for n in range(num_threads):
        threads[n] = DeleteThread(delete_counter, options, 'thread %d' % (n+1))
        threads[n].start()

    n = 0
    obj_list = Parse.query.Query.objects('Events', query, conn)[0]
    if len(obj_list) == 0:
        print 'No object matches the query'
    while len(obj_list) > 0:
        # Dispatch objects to threads for deletion
        for obj in obj_list:
            threads[n].obj_queue.put(obj)
            n = (n + 1) % num_threads

        # Wait for thread to finish before starting the next block
        # This is less efficient but avoid queue built up
        for n in range(num_threads):
            threads[n].obj_queue.join()
            print 'Thread %d queue empty' % (n + 1)

        # Read the next block
        conn = Parse.connection.Connection(app_id=options.app_id, api_key=None, master_key=options.master_key)
        obj_list = Parse.query.Query.objects('Events', query, conn)[0]

    # Terminate threads
    for n in range(num_threads):
        threads[n].obj_queue.put(None)
        threads[n].join()

    return n


def count(options):
    conn = Parse.connection.Connection(app_id=options.app_id, api_key=options.api_key,
                                       session_token=options.session_token,
                                       master_key=options.master_key)

    # Set up query
    query = get_query(options)
    query.count = 1
    query.limit = 0
    query.keys = []

    obj_count = Parse.query.Query.objects('Events', query, conn)[1]
    if 0 <= obj_count <= 1:
        print '%d object matches the query' % obj_count
    else:
        print '%d objects match the query' % obj_count
    return obj_count


def setup_event_formatter(cls, options):
    formatter = cls()
    for event_type in events.TYPES:
        if not hasattr(options, event_type.lower()):
            continue
        color = getattr(options, event_type.lower())
        if color is None:
            continue
        formatter.decorators[event_type] = \
            event_formatter.EventDecorator(event_formatter.AnsiDecorator(color=color))
    formatter.wbxml_tool_path = options.wbxml_tool_path
    return formatter


def query_(options):
    conn = Parse.connection.Connection(app_id=options.app_id, api_key=options.api_key,
                                       session_token=options.session_token)

    # Set up query
    query = get_query(options)
    query.limit = 1000
    query.skip = 0

    formatter = setup_event_formatter(event_formatter.RecordStyleEventFormatter, options)

    n = 0
    obj_list = Parse.query.Query.objects('Events', query, conn)[0]
    if options.limit and (len(obj_list) > options.limit):
        obj_list = obj_list[:options.limit]  # trim it down
    if len(obj_list) == 0:
        print 'No object matches the query'
    while len(obj_list) > 0:
        # Print out the result
        for obj in obj_list:
            print formatter.format(obj)

        # Read the next block
        query.skip += query.limit
        # Create a new connection because the old one may have timed out already
        conn = Parse.connection.Connection(app_id=options.app_id, api_key=options.api_key,
                                           session_token=options.session_token)
        obj_list = Parse.query.Query.objects('Events', query, conn)[0]
    return n


def console(options):
    # Set up current time
    current = UtcDateTime.now()

    formatter = setup_event_formatter(event_formatter.LogStyleEventFormatter, options)

    try:
        while True:
            # Query for all entries after current time
            query = get_query(options)
            query.add('createdAt', Parse.query.SelectorGreaterThan(current))
            conn = Parse.connection.Connection(app_id=options.app_id, api_key=options.api_key,
                                               session_token=options.session_token)
            obj_list = Parse.query.Query.objects('Events', query, conn)[0]

            if len(obj_list) == 0:
                # We are completely caught up. Sleep for a little bit
                time.sleep(3)
                continue

            # Update time
            current = UtcDateTime(obj_list[-1]['createdAt'])

            # Print out the event
            for obj in obj_list:
                # Strip createdAt
                obj.pop('createdAt')
                print formatter.format(obj)
    except KeyboardInterrupt:
        print 'Goodbye!'


def email(options):
    conn = Parse.connection.Connection(app_id=options.app_id, api_key=options.api_key,
                                       session_token=options.session_token)

    # Set up query
    query = Parse.query.Query()
    assert len(options.field) == len(options.selectors)
    for n in range(len(options.field)):
        if options.field[n] != 'timestamp':
            continue
        query.add(options.field[n], options.selectors[n])
    query.add('event_type', Parse.query.SelectorEqual('SUPPORT'))
    query.keys = options.display
    query.limit = 1000
    query.skip = 0

    obj_list = Parse.query.Query.objects('Events', query, conn)[0]
    (obfuscated, email_events) = support.Support.get_sha256_email_address(obj_list, options.email)
    print 'Email address: %s' % options.email
    print 'Obfuscated email address: %s' % obfuscated
    for event in email_events:
        print '%s: %s' % (event.timestamp, event.client)


def setup_keys(options, config_):
    # Determine if keys are already set up
    if options.app_id is not None and options.api_key is not None:
        print 'App ID and API key are already set up!'
        return

    # Get app id and api key
    print 'parse.py will try to set up your parse.cfg.'
    print ''
    print 'We need to set up Parse keys. Please log into your Parse account'
    print 'and locate your keys:'
    print ''
    print '1. Log into https://www.parse.com/.'
    print '2. Click "Welcome [your name]" around the upper right corner.'
    print '3. You should see "NachoMail" as one of the apps. Select it.'
    print '4. Click "Settings".'
    print '5. Click "Application Keys" on the left.'
    print '6. Please enter keys (from Parse) when prompted.'
    print ''
    options.app_id = raw_input('Application ID: ')
    options.api_key = raw_input('REST API Key: ')
    config_.write_keys(options)


def setup_session_token(options, config_):
    if options.session_token is not None:
        print 'Session token is already set up!'
        return

    # Log in and get session token
    print ''
    print 'Now, we need to log in as user "%s" to get its session token.' % options.username
    options.password = getpass.getpass('Enter the "well-known" password: ')
    conn = Parse.connection.Connection(app_id=options.app_id, api_key=options.api_key)
    user = Parse.users.User.login(username=options.username, password=options.password, conn=conn)
    options.session_token = user.session_token
    config_.write_keys(options)


def setup_test_query(options):
    # Make a test query to make sure it is working
    conn = Parse.connection.Connection(app_id=options.app_id,
                                       api_key=options.api_key,
                                       session_token=options.session_token)
    query = Parse.query.Query()
    query.add('event_type', Parse.query.SelectorEqual('INFO'))
    query.limit = 1
    event = Parse.query.Query.objects('Events', query, conn)[0]
    if len(event) != 1:
        print 'Fail to find an event. The credential may be invalid.'
        exit(1)
    print 'Query works!'


def setup_wbxml_tool(options, config_):
    if options.wbxml_tool_path is not None:
        print 'WbxmlTool is already set up!'
        return

    print ''
    print 'WbxmlTool.exe is a tool for decoding WBXML requests / responses in ' \
          'telemetry events. You can build your own copy in WbxmlTool.Mac project in ' \
          'NachoClientX solution.'
    print ''
    print 'parse.py needs the absolute path to WbxmlTool.exe. After build it, you ' \
          'should find the binary in NachoClientX/WbxmlTool.Mac/bin/Debug/.'
    while not options.wbxml_tool_path:
        path = raw_input('WbxmlTool path [press Enter to skip]: ')
        if path == '':
            print 'No WbxmlTool is configured. parse.py still works but WBXML decoding is not available.'
            return
        if not os.path.exists(path):
            print 'ERROR: file path does not exist.'
            continue
        if os.path.isdir(path):
            print 'ERROR: file path is a directory'
            continue
        options.wbxml_tool_path = path
    config_.write_wbxml_tool(options)


def setup(options, config_):
    if os.path.exists(options.config):
        os.chmod(options.config, 0600)

    setup_keys(options, config_)
    setup_session_token(options, config_)
    setup_test_query(options)
    setup_wbxml_tool(options, config_)

    # Keys are sensitive. Make sure only the user can read it.
    os.chmod(options.config, 0400)


class SelectorAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        if option_string == '--equal':
            sel = Parse.query.SelectorEqual(value)
        elif option_string == '--exists':
            value = value.strip()
            if value == 'true':
                sel = Parse.query.SelectorExists(True)
            elif value == 'false':
                sel = Parse.query.SelectorExists(False)
            else:
                raise ValueError('invalid boolean value %s' % value)
        elif option_string == '--after':
            sel = Parse.query.SelectorGreaterThanEqual(UtcDateTime(value))
        elif option_string == '--before':
            if value == 'now':
                sel = Parse.query.SelectorLessThan(UtcDateTime.now())
            else:
                sel = Parse.query.SelectorLessThan(UtcDateTime(value))
        elif option_string == '--startswith':
            sel = Parse.query.SelectorStartsWith(value)
        else:
            raise ValueError('unknown option %s' % option_string)
        getattr(namespace, self.dest).append(sel)


class FieldSelectorAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        if option_string == '--event_type':
            field = 'event_type'
            sel = Parse.query.SelectorEqual(value)
        elif option_string == '--client':
            field = 'client'
            sel = Parse.query.SelectorEqual(value)
        else:
            raise ValueError('unknown option %s' % option_string)
        getattr(namespace, 'field').append(field)
        getattr(namespace, 'selectors').append(sel)


def main():
    command_mapping = {'console': console,
                       'count': count,
                       'delete': delete,
                       'login': login,
                       'query': query_,  # add _ to avoid local var query shadows the function name
                       'setup': setup,
                       'email': email}

    valid_commands = sorted(command_mapping.keys())

    parser = argparse.ArgumentParser(add_help=False)

    # Access credential
    credential_group = parser.add_argument_group(title='Credential Options',
                                                 description='Option for configuring various keys.')
    credential_group.add_argument('--config', help='Configuration file', default='parse.cfg')

    # Login options
    login_group = parser.add_argument_group(title='Login Options',
                                            description='Options for login command.')
    login_group.add_argument('--password', help='Password. If none is given and it is needed, it will prompt for one',
                             default=None)
    login_group.add_argument('--username', help='Username', default='monitor')

    # Query options
    query_group = parser.add_argument_group(title='Query Options',
                                            description='A query consists of a list of expression. It is an AND '
                                                        'condition of all expressions. Each expression consists of a '
                                                        'field, an operator and a value. Use --field to specify the '
                                                        'field and other options (e.g. --equal) to specify the '
                                                        'operator and value. For example, --field event_type --equal '
                                                        'INFO means event_type == "INFO".')

    query_group.add_argument('--after',
                             help='Match a field after (and including) a time specified in ISO-8601 UTC format',
                             action=SelectorAction, dest='selectors', default=[])
    query_group.add_argument('--before', help='Match a field before (but excluding) a time specified '
                                              'in ISO-8601 UTC format or "now" for current time',
                             action=SelectorAction, dest='selectors', default=[])
    query_group.add_argument('--equal', help='Match a field to be equal to this value',
                             action=SelectorAction, dest='selectors', default=[])
    query_group.add_argument('--exists', help='Match a field to exist or not exist', choices=['true', 'false'],
                             action=SelectorAction, dest='selectors', default=[])
    query_group.add_argument('--field', help='A field for query (%s)' % ', ' .join(events.VALID_FIELDS),
                             action='append', metavar='FIELD',
                             choices=events.VALID_FIELDS, default=[])
    query_group.add_argument('--startswith', help='Match a field that starts with this string',
                             action=SelectorAction, dest='selectors', default=[])
    query_group.add_argument('--query', help='Match expression')

    # Query shorthands
    query_shorthand_group = parser.add_argument_group(title='Query Shorthands')
    query_shorthand_group.add_argument('--event_type', help='Same as --field event_type --equal EVENT_TYPE',
                                       action=FieldSelectorAction)
    query_shorthand_group.add_argument('--client', help='Same as --field client --equal CLIENT',
                                       action=FieldSelectorAction)

    # Filtering options
    filter_group = parser.add_argument_group(title='Filter Options')
    filter_group.add_argument('--limit', help='Upper limit of the # of entries to return', type=int,
                              default=None)
    filter_group.add_argument('--display', help='A list of all field to display',
                              metavar='FIELD', action='append',
                              choices=events.VALID_FIELDS, default=[])

    # Miscellaneous options
    misc_group = parser.add_argument_group(title='Miscellaneous Options')
    misc_group.add_argument('--email', help='Email address for email command')
    misc_group.add_argument('-h', '--help', help='Print this help message', action='store_true', dest='help')
    misc_group.add_argument('--wbxml-tool', metavar='PATH', help='File path to WbxmlTool', dest='wbxml_tool_path')

    # Command
    command_group = parser.add_argument_group(title='Commands',
                                              description='Actions to take after query returns a list of matching'
                                                          'events.\n\n'
                                                          'console - Stream the events to stdout to mimic Xcode'
                                                          'organizer console.\n'
                                                          'count - Return the number of events matching the query.\n'
                                                          'delete - Delete the matching events.\n'
                                                          'login - Print the user information after authentication.'
                                                          'This is used for getting the session token of user '
                                                          '"monitor".\n')
    command_group.add_argument('command', nargs='?', help='Choices: ' + ', '.join(valid_commands),
                               choices=valid_commands, metavar='COMMAND')

    options = parser.parse_args()

    if options.help or options.command is None:
        parser.print_help()
        exit(0)

    # We need to go thru the selectors and convert the value to a different
    # type if necessary. For example, --equal for timestamp should be converted to
    # a UtcDateTime object not a string.
    if len(options.field) != len(options.selectors):
        abort('ERROR: the number of fields does not match the number of selectors.')
    for (field, sel) in zip(options.field, options.selectors):
        if field in ['timestamp', 'createdAt', 'updatedAt', 'counter_start', 'counter_end']:
            if issubclass(sel.__class__, Parse.query.SelectorCompare) and isinstance(sel.value, str):
                sel.value = UtcDateTime(sel.value)
        elif field in ['count', 'min', 'max', 'average', 'stddev']:
            if issubclass(sel.__class__, Parse.query.SelectorCompare):
                sel.value = int(sel.value)

    config_file = Config(options.config)
    ColorsConfig(config_file).read(options)
    WbxmlToolConfig(config_file).read(options)

    # If there is no display field, set up the default
    if len(options.display) == 0:
        options.display = ['timestamp',
                           'event_type',
                           'client'] + events.INFO_FIELDS

    # Handle setup command separately because it takes an extra
    # parameter
    if options.command == 'setup':
        setup(options, config_file)
        exit(0)

    if options.command not in command_mapping:
        raise ValueError('unknown command %s' % options.command)
    command = command_mapping[options.command]
    command(options)

if __name__ == '__main__':
    main()