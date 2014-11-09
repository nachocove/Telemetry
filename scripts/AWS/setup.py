#!/usr/bin/env python

import sys
from datetime import datetime, timedelta
from argparse import ArgumentParser
from boto.dynamodb2.layer1 import DynamoDBConnection
from tables import TelemetryTable
from boto.exception import JSONResponseError


def write(s):
    sys.stdout.write(s)
    sys.stdout.flush()


def poll(fn, duration):
    then = datetime.now()
    while not fn():
        now = datetime.now()
        if (now - then).seconds > duration:
            then = now
            write('.')
    write('\n')


def get_table_names(connection):
    tables = connection.list_tables()[u'TableNames']
    return [str(x) for x in tables]


def list_tables(connection):
    tables = connection.list_tables()[u'TableNames']
    for table_name in tables:
        table_cls = TelemetryTable.find_table_class(table_name)
        print '-' * 72
        table = table_cls(connection)
        print table


def delete_tables(connection):
    tables = connection.list_tables()[u'TableNames']
    for table in tables:
        write('Deleting %s...' % table)
        try:
            connection.delete_table(table)
        except JSONResponseError, e:
            if u'message' in e.body and \
                e.body[u'message'].startswith(u'Attempt to change a resource which is still in use: '
                                              u'Table is being deleted: '):
                pass
            else:
                raise e

        def is_deleted():
            return table not in get_table_names(connection)
        poll(is_deleted, 1)


def create_tables(connection):
    for cls in TelemetryTable.TABLE_CLASSES:
        try:
            table_name = TelemetryTable.full_table_name(cls.TABLE_NAME)
            write('Creating %s...' % table_name)
            table = cls.create_table(connection)
        except JSONResponseError, e:
            if ((u'Message' in e.body and e.body[u'Message'] == u'Cannot create preexisting table') or
                (u'message' in e.body and e.body[u'message'].startswith(u'Table already exists:'))):
                print 'Table %s already exists.' % cls.TABLE_NAME
                table = cls(connection)
            else:
                raise e
        poll(table.is_active, 1)


def main():
    parser = ArgumentParser()
    parser.add_argument('--host',
                        help='Host of AWS DynamoDB instance (Default: dynamodb.us-west-2.amazonaws.com)',
                        default='localhost')
    parser.add_argument('--port',
                        help='Port of AWS DynamoDB instance (Default: 443)',
                        default=443)
    parser.add_argument('--secret-key',
                        help='AWS secret access key',
                        default=None)
    parser.add_argument('--access-key',
                        help='AWS access key id',
                        default=None)
    parser.add_argument('--prefix',
                        help='Prefix of the table names',
                        default='dev')
    parser.add_argument('--local',
                        help='Use local DynamoDB',
                        action='store_true',
                        default=False)
    parser.add_argument('action',
                        metavar='ACTION',
                        nargs='+',
                        help='List of actions. (Choices are: list, reset, setup)',
                        choices=['list', 'reset', 'setup'])
    options = parser.parse_args()

    if options.local:
        options.access_key = 'dynamodb_local'
        options.secret_key = 'dynamodb_local'
        options.host = 'localhost'
        options.port = 8000
    is_secure = True
    if options.host == 'localhost':
        is_secure = False

    TelemetryTable.PREFIX = options.prefix

    conn = DynamoDBConnection(host=options.host,
                              port=options.port,
                              aws_secret_access_key=options.secret_key,
                              aws_access_key_id=options.access_key,
                              region='us-west-2',
                              is_secure=is_secure)

    action_table = {
        'list': list_tables,
        'reset': delete_tables,
        'setup': create_tables
    }

    for action in options.action:
        action_fn = action_table.get(action, None)
        if action_fn is None:
            print 'Ignore unknown action %s' % action
            continue
        assert callable(action_fn)
        action_fn(conn)

if __name__ == '__main__':
    main()
