from argparse import ArgumentParser
import Parse
import getpass
import pprint


def abort(mesg):
    print 'ERROR: ' + mesg
    exit(1)


def login(options):
    """
    Log in a user and print the uesr info.
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


def main():
    parser = ArgumentParser()
    parser.add_argument('--api-key', help='REST API key [default: NachoMail API key]',
                        default='krLhpUrvcoKXTNx8LWG8ESR1zQGRzei6vttCmwFm')
    parser.add_argument('--app-id', help='application ID',
                        default='rhBQMv6X1qBRIgb32LFoEnDpeQw3xiOuXjJL224v')
    parser.add_argument('--username', help='username', default=None)
    parser.add_argument('--password', help='password. If none is given and it is needed, it will prompt for one',
                        default=None)
    parser.add_argument('command', nargs='?', help='commands: login, user, query')
    options = parser.parse_args()

    #options.app_id = 'D4Wb9PGYb9gSXNa6Te4Oy31QF7ANnE4uAA9S9F4G'
    #options.api_key = '0xH5KQTdOGnzB8sXcfwAmIrSNJYnsuYrO8ZPuzbt'

    if options.command == 'login':
        login(options)

if __name__ == '__main__':
    main()