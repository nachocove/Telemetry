import argparse
import Parse
import getpass
import pprint


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


def delete(options):
    """
    Delete all objects that match the query.
    """
    if not options.master_key:
        abort('master key is required for deletion')

    conn = Parse.connection.Connection(app_id=options.app_id, api_key=None, master_key=options.master_key)

    # Set up query
    query = get_query(options)
    query.keys = ''  # We only want the objectId

    n = 0
    obj_list = Parse.query.Query.objects('Events', query, conn)[0]
    if len(obj_list) == 0:
        print 'No object matches the query'
    while len(obj_list) > 0:
        # Delete them id by id
        for obj in obj_list:
            assert obj.id is not None
            n += 1
            print '[%d] Deleting object %s...' % (n, obj.id)
            obj.delete(conn)

        # Read the next block
        obj_list = Parse.query.Query.objects('Events', query, conn)[0]
    return n


def count(options):
    conn = Parse.connection.Connection(app_id=options.app_id, api_key=options.api_key,
                                       session_token=options.session_token,
                                       master_key=options.master_key)

    # Set up query
    query = get_query(options)
    query.count = 1
    query.limit = 0

    obj_count = Parse.query.Query.objects('Events', query, conn)[1]
    if 0 <= obj_count <= 1:
        print '%d object matches the query' % obj_count
    else:
        print '%d objects match the query' % obj_count
    return obj_count


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
                assert False
        else:
            assert False
        getattr(namespace, self.dest).append(sel)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--app-id', help='application ID',
                        required=True)
    rest_api_key_group = parser.add_mutually_exclusive_group(required=True)
    rest_api_key_group.add_argument('--api-key', help='REST API key')
    rest_api_key_group.add_argument('--master-key', help='Master key')

    # Login options
    parser.add_argument('--username', help='username', default=None)
    parser.add_argument('--password', help='password. If none is given and it is needed, it will prompt for one',
                        default=None)
    parser.add_argument('--session-token', help='session token', default=None)

    # Query options
    parser.add_argument('--field', help='a field for query', action='append', default=[])
    parser.add_argument('--equal', help='match a field to be equal to this value',
                        action=SelectorAction, dest='selectors', default=[])
    parser.add_argument('--exists', help='match a field to exist',
                        action=SelectorAction, dest='selectors', default=[])
    parser.add_argument('--limit', help='upper limit of the # of entries to return', type=int,
                        default=None)
    parser.add_argument('command', nargs='?', help='command',
                        choices=['count', 'delete', 'login', 'query'])

    options = parser.parse_args()

    if options.command == 'login':
        login(options)
    elif options.command == 'delete':
        delete(options)
    elif options.command == 'count':
        count(options)
    else:
        assert False


if __name__ == '__main__':
    main()