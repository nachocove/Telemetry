#!/usr/bin/env python

import sys
import locale
from datetime import datetime
from argparse import ArgumentParser
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
from AWS.pricing import dynamodb_rate

sys.path.append('../')
from tables import TelemetryTable, TABLE_CLASSES
from boto.exception import JSONResponseError
from misc.config import Config
from config import AwsConfig


def write(s):
    sys.stdout.write(s)
    sys.stdout.flush()


def progress():
    write('.')


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


def list_tables(connection, options):
    tables = connection.list_tables()[u'TableNames']
    for table_name in tables:
        table_cls = TelemetryTable.find_table_class(table_name)
        print '-' * 72
        table = table_cls(connection)
        print table


def delete_tables(connection, options):
    for table in TABLE_CLASSES:
        table_name = TelemetryTable.full_table_name(table.TABLE_NAME)
        write('Deleting %s...' % table_name)
        try:
            connection.delete_table(table_name)
        except JSONResponseError, e:
            if u'message' in e.body:
                message = e.body[u'message']
                if message.startswith(u'Attempt to change a resource which is still in use: '
                                      u'Table is being deleted: '):
                    pass
                if message.startswith(u'Requested resource not found: Table: '):
                    print 'Table %s does not exist.' % table.TABLE_NAME
                    continue
            else:
                raise e

        def is_deleted():
            return table_name not in get_table_names(connection)
        poll(is_deleted, 1)


def create_tables(connection, options):
    for cls in TABLE_CLASSES:
        try:
            table_name = TelemetryTable.full_table_name(cls.TABLE_NAME)
            write('Creating %s...' % table_name)
            cls.create_table(connection, polling_fn=progress)
            write('\n')
        except JSONResponseError, e:
            if ((u'Message' in e.body and e.body[u'Message'] == u'Cannot create preexisting table') or
                    (u'message' in e.body and e.body[u'message'].startswith(u'Table already exists:'))):
                print 'Table %s already exists.' % cls.TABLE_NAME
            else:
                raise e

def format_cost(cost):
    try:
        # try setting user's locale
        locale.setlocale(locale.LC_ALL, '')
        return locale.currency(cost, grouping=True)
    except ValueError:
        # didn't work. Set USA
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        return locale.currency(cost, grouping=True)

def show_table_cost(connection, options):

    total_read_units = 0
    total_write_units = 0
    total_read_cost = 0.0
    total_write_cost = 0.0

    tables = connection.list_tables()[u'TableNames']
    print ' Read Write       Read      Write   Table'
    print ' Unit  Unit       Cost       Cost'
    print '----- ----- ---------- ----------   ----------------------------'

    for table_name in tables:
        if options.prefix and not table_name.startswith(options.prefix):
            continue
        if options.table and not table_name.endswith(options.table):
            continue

        table = Table(table_name=table_name, connection=connection)
        info = table.describe()[u'Table']

        read_units = info[u'ProvisionedThroughput'][u'ReadCapacityUnits']
        write_units = info[u'ProvisionedThroughput'][u'WriteCapacityUnits']
        read_cost = read_units * dynamodb_rate * 24 * 31
        write_cost = write_units * dynamodb_rate * 24 * 31

        total_read_units += read_units
        total_write_units += write_units
        total_read_cost += read_cost
        total_write_cost += write_cost

        print '%5d %5d %10s %10s   %s' % (read_units, write_units, format_cost(read_cost),
                                          format_cost(write_cost), table_name)

        for index_item in info['GlobalSecondaryIndexes']:
            read_units = index_item[u'ProvisionedThroughput'][u'ReadCapacityUnits']
            write_units = info[u'ProvisionedThroughput'][u'WriteCapacityUnits']
            read_cost = read_units * dynamodb_rate * 24 * 31
            write_cost = write_units * dynamodb_rate * 24 * 31

            total_read_units += read_units
            total_write_units += write_units
            total_read_cost += read_cost
            total_write_cost += write_cost

            print '%5d %5d %10s %10s   %s' % (read_units, write_units, format_cost(read_cost),
                                              format_cost(write_cost), "%s-%s" % (table_name, index_item['IndexName']))

    print '%5d %5d %10s %10s   TOTAL' % (total_read_units, total_write_units, format_cost(total_read_cost),
                                         format_cost(total_write_cost))
    print '\nTotal number of units = %d' % (total_read_units + total_write_units)
    print 'Total cost = %s' % format_cost(total_read_cost + total_write_cost)


def main():
    parser = ArgumentParser()
    parser.add_argument('--host',
                        help='Host of AWS DynamoDB instance (Default: dynamodb.us-west-2.amazonaws.com)',
                        default='dynamodb.us-west-2.amazonaws.com')
    parser.add_argument('--port',
                        help='Port of AWS DynamoDB instance (Default: 443)',
                        default=443)
    parser.add_argument('--secret-access-key', '-s',
                        help='AWS secret access key',
                        dest='aws_secret_access_key',
                        default=None)
    parser.add_argument('--access-key-id', '-a',
                        help='AWS access key id',
                        dest='aws_access_key_id',
                        default=None)
    parser.add_argument('--prefix', '-p',
                        help='Prefix of the table names',
                        default='')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--local',
                       help='Use local DynamoDB',
                       action='store_true',
                       default=False)
    group.add_argument('--config', '-c',
                       help='Configuration that contains an AWS section',
                       default=None)
    subparser = parser.add_subparsers()

    list_parser = subparser.add_parser('list')
    list_parser.set_defaults(func=list_tables)

    show_cost_parser = subparser.add_parser('show-cost')
    show_cost_parser.set_defaults(func=show_table_cost)
    show_cost_parser.add_argument('--table', help='Specific table to look at',
                                  default='')

    reset_parser = subparser.add_parser('reset')
    reset_parser.set_defaults(func=delete_tables)

    setup_parser = subparser.add_parser('setup')
    setup_parser.set_defaults(func=create_tables)

    options = parser.parse_args()

    if options.local:
        options.aws_access_key_id = 'dynamodb_local'
        options.aws_secret_access_key = 'dynamodb_local'
        options.host = 'localhost'
        options.port = 8000
    if options.config:
        config_file = Config(options.config)
        AwsConfig(config_file).read(options)

    is_secure = True
    if options.host == 'localhost':
        is_secure = False

    TelemetryTable.PREFIX = options.prefix

    if not options.aws_access_key_id or not options.aws_secret_access_key:
            print "ERROR: No access-key or secret key. Need either a config or aws_access_key_id and aws_secret_access_key."
            sys.exit(1)
    conn = DynamoDBConnection(host=options.host,
                              port=options.port,
                              aws_secret_access_key=options.aws_secret_access_key,
                              aws_access_key_id=options.aws_access_key_id,
                              region='us-west-2',
                              is_secure=is_secure)
    if not options.func:
        raise ValueError('no function defined for argument')

    options.func(conn, options)

if __name__ == '__main__':
    main()
