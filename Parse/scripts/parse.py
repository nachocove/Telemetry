import argparse
import Parse
import getpass
import pprint
import config
import threading
import Queue


def abort(mesg):
    print 'ERROR: ' + mesg
    exit(1)


def get_query(options):
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
    This class provides a thread-safe counter that can provide a
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


def query_(options):
    conn = Parse.connection.Connection(app_id=options.app_id, api_key=options.api_key,
                                       session_token=options.session_token)

    # Set up query
    query = get_query(options)
    query.limit = 1000
    query.skip = 0

    pp = pprint.PrettyPrinter(depth=4)

    n = 0
    obj_list = Parse.query.Query.objects('Events', query, conn)[0]
    if options.limit and (len(obj_list) > options.limit):
        obj_list = obj_list[:options.limit]  # trim it down
    if len(obj_list) == 0:
        print 'No object matches the query'
    while len(obj_list) > 0:
        # Print out the result
        pp.pprint(obj_list)

        # Read the next block
        query.skip += query.limit
        obj_list = Parse.query.Query.objects('Events', query, conn)[0]
    return n


def console(options):
    pass


class SelectorAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        if option_string == '--equal':
            sel = Parse.query.SelectorEqual(value)
        elif option_string == '--exists':
            value = value.strip()
            if value == 'True':
                sel = Parse.query.SelectorExists(True)
            elif value == 'False':
                sel = Parse.query.SelectorExists(False)
            else:
                raise ValueError('invalid boolean value %s' % value)
        elif option_string == '--after':
            sel = Parse.utc_datetime.UtcDateTime(value)
        elif option_string == '--before':
            if value == 'now':
                sel = Parse.utc_datetime.UtcDateTime.now()
            else:
                sel = Parse.utc_datetime.UtcDateTime(value)
        else:
            raise ValueError('unknown option %s' % option_string)
        getattr(namespace, self.dest).append(sel)


def main():
    valid_fields = ['build_version',
                    'client',
                    'createdAt',
                    'device_model',
                    'event_type',
                    'message',
                    'objectId',
                    'os_type',
                    'os_version',
                    'timestamp',
                    'updatedAt',
                    'wbxml']

    command_mapping = {'console': console,
                       'count': count,
                       'delete': delete,
                       'login': login,
                       'query': query_}  # add _ to avoid local var query shadows the function name

    valid_commands = sorted(command_mapping.keys())

    parser = argparse.ArgumentParser()

    # Access credential
    credential_group = parser.add_argument_group(title='Credential Options',
                                                 description='Option for configuring various keys.')
    credential_group.add_argument('--app-id', help='Application ID', default=None)
    rest_api_key_group = credential_group.add_mutually_exclusive_group()
    rest_api_key_group.add_argument('--api-key', help='REST API key')
    credential_group.add_argument('--config', help='Configuration file', default='parse.cfg')
    rest_api_key_group.add_argument('--master-key', help='Master key')
    credential_group.add_argument('--session-token', help='Session token', default=None)

    # Login options
    login_group = parser.add_argument_group(title='Login Options',
                                            description='Options for login command.')
    login_group.add_argument('--password', help='Password. If none is given and it is needed, it will prompt for one',
                             default=None)
    login_group.add_argument('--username', help='Username', default=None)

    # Query options
    query_group = parser.add_argument_group(title='Query Options')
    query_group.add_argument('--after', help='Match a field after a time specified in ISO-8601 UTC format',
                             action=SelectorAction, dest='selectors', default=[])
    query_group.add_argument('--before', help='Match a field before a time specified '
                                              'in ISO-8601 UTC format or "now" for current time',
                             action=SelectorAction, dest='selectors', default=[])
    query_group.add_argument('--equal', help='Match a field to be equal to this value',
                             action=SelectorAction, dest='selectors', default=[])
    query_group.add_argument('--exists', help='Match a field to exist',
                             action=SelectorAction, dest='selectors', default=[])
    query_group.add_argument('--field', help='A field for query (%s)' % ', ' .join(valid_fields),
                             action='append', metavar='FIELD',
                             choices=valid_fields, default=[])

    # Filtering options
    filter_group = parser.add_argument_group(title='Filter Options')
    filter_group.add_argument('--limit', help='Upper limit of the # of entries to return', type=int,
                              default=None)
    filter_group.add_argument('--display', help='A list of all field to display',
                              metavar='FIELD', action='append',
                              choices=valid_fields, default=[])

    parser.add_argument('command', nargs='?', help='Choices: ' + ', '.join(valid_commands),
                        choices=valid_commands, metavar='COMMAND')

    options = parser.parse_args()

    def has_credential(opt):
        if opt.app_id is None:
            return False
        if (opt.api_key is None) and (opt.master_key is None):
            return False
        return True

    # Sanity check parameters
    # Make sure we have keys
    config_ = config.Config(options.config)
    if not has_credential(options):
        config_.read_keys(options)
    else:
        # Write the credential
        config_.write_keys(options)

    # If there is no display fields, set up the default
    if len(options.display) == 0:
        options.display = ['objectId', 'timestamp', 'event_type', 'message', 'wbxml']

    if options.command not in globals():
        raise ValueError('unknown command %s' % options.command)
    globals()[options.command](options)

if __name__ == '__main__':
    main()