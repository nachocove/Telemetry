import getpass
import pprint

from argparse import ArgumentParser
from Parse.connection import Connection
from Parse.users import User
from Parse.roles import Role
from Parse.acl import Acl


def create_user(conn, username, password, email=None):
    user = User(username=username, password=password, email=email, conn=conn)
    user.create()
    user_data = user.data()
    user_data['password'] = '*' * len(user_data['password'])
    pprint.PrettyPrinter(depth=4).pprint(user_data)
    return user


def create_role(conn, user):
    role = Role(name='Ops', conn=conn)
    role.acl = Acl()
    role.acl.add('*', read=True, write=False)
    role.users.add(user)
    role.create()
    pprint.PrettyPrinter(depth=4).pprint(role.data())


def main():
    parser = ArgumentParser()
    parser.add_argument('--app-id', help='application ID')
    parser.add_argument('--api-key', help='REST API key')
    parser.add_argument('--username', help='username of the user account', default='monitor')
    parser.add_argument('--email', help='email address for the user account')
    options = parser.parse_args()

    print options

    # Create the connection to be used
    conn = Connection(app_id=options.app_id, api_key=options.api_key)

    print 'Username: %s' % options.username
    password = getpass.getpass('Password: ')

    print '\nCreating user...'
    user = create_user(conn, options.username, password)
    print '\nCreating role...'
    create_role(conn, user)

if __name__ == '__main__':
    main()