#!/usr/bin/env python

import pprint
from argparse import ArgumentParser
from boto.dynamodb2.layer1 import DynamoDBConnection
from model import *
from boto.exception import JSONResponseError


def list_tables(connection):
    tables = connection.list_tables()[u'TableNames']
    pretty = pprint.PrettyPrinter(indent=2)
    for table_name in tables:
        table = Table(table_name=table_name, connection=connection)
        info = table.describe()
        info[u'Name'] = table_name
        info[u'Rows'] = table.count()
        pretty.pprint(info)


def delete_tables(connection):
    tables = connection.list_tables()[u'TableNames']
    for table in tables:
        print 'Deleting %s...' % table
        connection.delete_table(table)


def create_tables(connection):
    table_classes = [
        Log,
        Wbxml,
        Counter,
        Capture,
        Support,
        Ui
    ]

    for cls in table_classes:
        try:
            print 'Creating %s...' % TelemetryTable.full_table_name(cls.TABLE_NAME)
            cls.create_table(connection)
        except JSONResponseError, e:
            if e.body[u'Message'] == u'Cannot create preexisting table':
                print 'Table already exists.'
            else:
                raise e


def main():
    parser = ArgumentParser()
    parser.add_argument('--host',
                        help='Host of AWS DynamoDB instance (Default: localhost)',
                        default='localhost')
    parser.add_argument('--port',
                        help='Port of AWS DynamoDB instance (Default: 8000)',
                        default=8000)
    parser.add_argument('--secret-key',
                        help='AWS secret access key',
                        default='local_db')
    parser.add_argument('--access-key',
                        help='AWS access key id',
                        default='local_db')
    parser.add_argument('--prefix',
                        help='Prefix of the table names',
                        default='test')
    parser.add_argument('action',
                        metavar='ACTION',
                        nargs='+',
                        help='List of actions. (Choices are: list, reset, setup)',
                        choices=['list', 'reset', 'setup'])
    options = parser.parse_args()

    is_secure = True
    if options.host == 'localhost':
        is_secure = False

    TelemetryTable.PREFIX = options.prefix

    conn = DynamoDBConnection(host=options.host,
                              port=options.port,
                              aws_secret_access_key=options.secret_key,
                              aws_access_key_id=options.access_key,
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
        action_fn(conn)

if __name__ == '__main__':
    main()