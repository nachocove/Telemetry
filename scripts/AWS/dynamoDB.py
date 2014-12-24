# Copyright 2014, NachoCove, Inc
from datetime import datetime
import locale

from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
import sys

from AWS.config import CliFunc
from pricing import dynamodb_write_rate, dynamodb_read_rate


from tables import TelemetryTable, TABLE_CLASSES
from boto.exception import JSONResponseError

class DynamoDbCliFunc(CliFunc):
    def run(self, args, **kwargs):
        super(DynamoDbCliFunc, self).run(args, **kwargs)
        if args.local:
            args.aws_access_key_id = 'dynamodb_local'
            args.aws_secret_access_key = 'dynamodb_local'
            args.host = 'localhost'
            args.port = 8000
        is_secure = False if args.host == 'localhost' else True
        if not args.aws_prefix:
            print "ERROR: need to have a prefix. Set one on the Command-Line or in the config file."
        self.prefix = TelemetryTable.PREFIX = args.aws_prefix

        if not args.aws_access_key_id or not args.aws_secret_access_key:
            print "ERROR: No access-key or secret key. Need either a config or aws_access_key_id and aws_secret_access_key."
            sys.exit(1)

        self.connection = DynamoDBConnection(host=args.host,
                                             port=args.port,
                                             aws_secret_access_key=args.aws_secret_access_key,
                                             aws_access_key_id=args.aws_access_key_id,
                                             region='us-west-2',
                                             is_secure=is_secure)

    def get_table_names(self):
        tables = self.connection.list_tables()
        return [str(x) for x in tables[u'TableNames'] if not self.prefix or x.startswith(self.prefix)]



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


class ListTables(DynamoDbCliFunc):
    def add_arguments(self, parser, subparser):
        sub = subparser.add_parser('list-tables')
        sub.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(ListTables, self).run(args, **kwargs)
        return self.list_tables(args)

    def list_tables(self, options):
        tables = self.get_table_names()
        for table_name in tables:
            table_cls = TelemetryTable.find_table_class(table_name)
            print '-' * 72
            table = table_cls(self.connection)
            print table


class DeleteTables(DynamoDbCliFunc):
    def add_arguments(self, parser, subparser):
        sub = subparser.add_parser('delete-tables')
        sub.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(DeleteTables, self).run(args, **kwargs)
        return self.delete_tables(args)

    def delete_tables(self, options):
        for table in TABLE_CLASSES:
            table_name = TelemetryTable.full_table_name(table.TABLE_NAME)
            write('Deleting %s...' % table_name)
            try:
                self.connection.delete_table(table_name)
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
                return table_name not in self.get_table_names()
            poll(is_deleted, 1)

class CreateTables(DynamoDbCliFunc):
    def add_arguments(self, parser, subparser):
        sub = subparser.add_parser('create-tables')
        sub.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(CreateTables, self).run(args, **kwargs)
        return self.create_tables(args)

    def create_tables(self, options):
        for cls in TABLE_CLASSES:
            try:
                table_name = TelemetryTable.full_table_name(cls.TABLE_NAME)
                write('Creating %s...' % table_name)
                cls.create_table(self.connection, polling_fn=progress)
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

class ShowTableCost(DynamoDbCliFunc):
    def add_arguments(self, parser, subparser):
        show_cost_parser = subparser.add_parser('show-cost')
        show_cost_parser.add_argument('--table', help='Specific table to look at',
                                      default='')
        show_cost_parser.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(ShowTableCost, self).run(args, **kwargs)
        return self.show_table_cost(args)

    def show_table_cost(self, options):

        total_read_units = 0
        total_write_units = 0
        total_read_cost = 0.0
        total_write_cost = 0.0

        tables = self.get_table_names()
        print ' Read Write       Read      Write   Table'
        print ' Unit  Unit       Cost       Cost'
        print '----- ----- ---------- ----------   ----------------------------'

        for table_name in tables:
            if options.table and not table_name.endswith(options.table):
                continue

            table = Table(table_name=table_name, connection=self.connection)
            info = table.describe()[u'Table']

            read_units = info[u'ProvisionedThroughput'][u'ReadCapacityUnits']
            write_units = info[u'ProvisionedThroughput'][u'WriteCapacityUnits']
            read_cost = read_units * dynamodb_read_rate * 24 * 31
            write_cost = write_units * dynamodb_write_rate * 24 * 31

            total_read_units += read_units
            total_write_units += write_units
            total_read_cost += read_cost
            total_write_cost += write_cost

            print '%5d %5d %10s %10s   %s' % (read_units, write_units, format_cost(read_cost),
                                              format_cost(write_cost), table_name)

            for index_item in info['GlobalSecondaryIndexes']:
                read_units = index_item[u'ProvisionedThroughput'][u'ReadCapacityUnits']
                write_units = info[u'ProvisionedThroughput'][u'WriteCapacityUnits']
                read_cost = read_units * dynamodb_read_rate * 24 * 31
                write_cost = write_units * dynamodb_write_rate * 24 * 31

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
        print '(all costs are for 31 days).'
