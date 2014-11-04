from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex
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
    def create(cls, connection, local_secondary_indexes=None, global_secondary_indexes=None, throughput=None):
        # Schema is common for all tables - hash key on client id and range key on timestamp
        schema = [
            HashKey('id', data_type=STRING),
        ]

        if global_secondary_indexes is None:
            global_secondary_indexes = list()
        client_timestamp_index = GlobalAllIndex('index.client-timestamp',
                                                parts=[
                                                    HashKey('client', data_type=STRING),
                                                    RangeKey('timestamp', data_type=NUMBER)
                                                ],
                                                throughput={
                                                    'read': 5,
                                                    'write': 5
                                                })
        global_secondary_indexes.append(client_timestamp_index)

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

    @classmethod
    def create_table(cls, connection):
        event_type_timestamp_index = GlobalAllIndex('index.event_type-timestamp',
                                                    parts=[
                                                    HashKey('event_type', data_type=STRING),
                                                    RangeKey('timestamp', data_type=NUMBER)
                                                    ],
                                                    throughput={
                                                    'read': 5,
                                                    'write': 5
                                                    })
        cls.create(connection, global_secondary_indexes=[event_type_timestamp_index])


class Wbxml(TelemetryTable):
    TABLE_NAME = 'wbxml'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Wbxml.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        event_type_timestamp_index = GlobalAllIndex('index.event_type-timestamp',
                                                    parts=[
                                                        HashKey('event_type', data_type=STRING),
                                                        RangeKey('timestamp', data_type=NUMBER)
                                                    ],
                                                    throughput={
                                                        'read': 5,
                                                        'write': 5
                                                    })
        cls.create(connection, global_secondary_indexes=[event_type_timestamp_index])


class Counter(TelemetryTable):
    TABLE_NAME = 'counter'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Counter.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        counter_name_timestamp_index = GlobalAllIndex('index.counter_name-timestamp',
                                                      parts=[
                                                          HashKey('counter_name', data_type=STRING),
                                                          RangeKey('timestamp', data_type=NUMBER)
                                                      ],
                                                      throughput={
                                                          'read': 5,
                                                          'write': 5
                                                      })
        cls.create(connection, global_secondary_indexes=[counter_name_timestamp_index])


class Capture(TelemetryTable):
    TABLE_NAME = 'capture'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Capture.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        capture_name_timestamp_index = GlobalAllIndex('index.capture_name-timestamp',
                                                      parts=[
                                                          HashKey('capture_name', data_type=STRING),
                                                          RangeKey('timestamp', data_type=NUMBER)
                                                      ],
                                                      throughput={
                                                          'read': 5,
                                                          'write': 5
                                                      })
        cls.create(connection, global_secondary_indexes=[capture_name_timestamp_index])


class Support(TelemetryTable):
    TABLE_NAME = 'support'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Support.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        cls.create(connection, global_secondary_indexes=None)


class Ui(TelemetryTable):
    TABLE_NAME = 'ui'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=Ui.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        cls.create(connection, global_secondary_indexes=None)
