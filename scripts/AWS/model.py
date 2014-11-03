from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.types import NUMBER, STRING


class TelemetryTable(Table):
    PREFIX = None
    TABLE_NAME = None

    def __init__(self, connection, table_name):
        Table.__init__(self, table_name=TelemetryTable.full_table_name(table_name),
                       connection=connection)

    @staticmethod
    def full_table_name(table):
        if TelemetryTable.PREFIX is None:
            raise ValueError('Prefix is not set yet.')
        return TelemetryTable.PREFIX + '.telemetry.' + table

    @classmethod
    def create_table(cls, connection, local_secondary_indexes=None, global_secondary_indexes=None, throughput=None):
        # Schema is common for all tables - hash key on client id and range key on timestamp
        schema = [
            HashKey('client', data_type=STRING),
            RangeKey('timestamp', data_type=NUMBER)
        ]
        if cls.TABLE_NAME is None:
            raise ValueError('TABLE_NAME is not properly overridden in derived class.')
        Table.create(table_name=TelemetryTable.full_table_name(cls.TABLE_NAME),
                     schema=schema,
                     throughput=throughput,
                     indexes=local_secondary_indexes,
                     global_indexes=global_secondary_indexes,
                     connection=connection)


class Log(TelemetryTable):
    TABLE_NAME = 'log'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Log.TABLE_NAME)


class Wbxml(TelemetryTable):
    TABLE_NAME = 'wbxml'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Wbxml.TABLE_NAME)


class Counter(TelemetryTable):
    TABLE_NAME = 'counter'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Counter.TABLE_NAME)


class Capture(TelemetryTable):
    TABLE_NAME = 'capture'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Capture.TABLE_NAME)


class Support(TelemetryTable):
    TABLE_NAME = 'support'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Support.TABLE_NAME)


class Ui(TelemetryTable):
    TABLE_NAME = 'ui'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Ui.TABLE_NAME)
