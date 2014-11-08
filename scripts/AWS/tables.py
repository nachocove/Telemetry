from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex
from boto.dynamodb2.types import NUMBER, STRING
from table_query import TelemetryTableQuery


class TelemetryTable(Table):
    PREFIX = None
    TABLE_NAME = None
    FIELD_NAMES = list()
    # These are fields that exist in all telemetry event tables
    # (but not in device info table)
    COMMON_FIELD_NAMES = ['id', 'client', 'timestamp']
    CLIENT_TIMESTAMP_INDEX = 'index.client-timestamp'

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
        client_timestamp_index = GlobalAllIndex(TelemetryTable.CLIENT_TIMESTAMP_INDEX,
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
        return cls(connection)

    @classmethod
    def is_for_us(cls, fields):
        for field in fields:
            if not (field in TelemetryTable.COMMON_FIELD_NAMES or field in cls.FIELD_NAMES):
                return False
        return True

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTableQuery()
        result.for_us = cls.is_for_us(query.selectors.keys())
        result.may_add_primary_hashkey(query.selectors, 'id')
        result.may_add_secondary_hashkey(query.selectors, 'client', TelemetryTable.CLIENT_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp',
                                          index_name=[
                                              TelemetryTable.CLIENT_TIMESTAMP_INDEX,
                                          ])
        result.set_query_filter(query)
        return result

    def is_active(self):
        info = self.describe()
        return info[u'Table'][u'TableStatus'] == u'ACTIVE'


class LogTable(TelemetryTable):
    TABLE_NAME = 'log'
    EVENT_TYPES = ['ERROR', 'WARN', 'INFO', 'DEBUG']
    FIELD_NAMES = ['event_type', 'message']
    EVENT_TYPE_TIMESTAMP_INDEX = 'index.event_type-timestamp'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=LogTable.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        event_type_timestamp_index = GlobalAllIndex(LogTable.EVENT_TYPE_TIMESTAMP_INDEX,
                                                    parts=[
                                                        HashKey('event_type', data_type=STRING),
                                                        RangeKey('timestamp', data_type=NUMBER)
                                                    ],
                                                    throughput={
                                                        'read': 5,
                                                        'write': 5
                                                    })
        return cls.create(connection, global_secondary_indexes=[event_type_timestamp_index])

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTable.should_handle(query)
        result.for_us = cls.is_for_us(query.selectors.keys())
        result.may_add_secondary_hashkey(query.selectors, 'event_type', cls.EVENT_TYPE_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp', [cls.EVENT_TYPE_TIMESTAMP_INDEX])
        result.set_query_filter(query)
        return result


class WbxmlTable(TelemetryTable):
    TABLE_NAME = 'wbxml'
    EVENT_TYPES = ['WBXML_REQUEST', 'WBXML_RESPONSE']
    FIELD_NAMES = ['event_type', 'wbxml']
    EVENT_TYPE_TIMESTAMP_INDEX = 'index.event_type-timestamp'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=WbxmlTable.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        event_type_timestamp_index = GlobalAllIndex(WbxmlTable.EVENT_TYPE_TIMESTAMP_INDEX,
                                                    parts=[
                                                        HashKey('event_type', data_type=STRING),
                                                        RangeKey('timestamp', data_type=NUMBER)
                                                    ],
                                                    throughput={
                                                        'read': 5,
                                                        'write': 5
                                                    })
        return cls.create(connection, global_secondary_indexes=[event_type_timestamp_index])

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTable.should_handle(query)
        result.for_us = cls.is_for_us(query.selectors.keys())
        result.may_add_secondary_hashkey(query.selectors, 'event_type', cls.EVENT_TYPE_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp', [cls.EVENT_TYPE_TIMESTAMP_INDEX])
        result.set_query_filter(query)
        return result


class CounterTable(TelemetryTable):
    TABLE_NAME = 'counter'
    FIELD_NAMES = ['counter_name', 'count', 'counter_start', 'counter_end']
    COUNTER_NAME_TIMESTAMP_INDEX = 'index.counter_name-timestamp'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=CounterTable.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        counter_name_timestamp_index = GlobalAllIndex(CounterTable.COUNTER_NAME_TIMESTAMP_INDEX,
                                                      parts=[
                                                          HashKey('counter_name', data_type=STRING),
                                                          RangeKey('timestamp', data_type=NUMBER)
                                                      ],
                                                      throughput={
                                                          'read': 5,
                                                          'write': 5
                                                      })
        return cls.create(connection, global_secondary_indexes=[counter_name_timestamp_index])

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTable.should_handle(query)
        result.for_us = cls.is_for_us(query.selectors.keys())
        result.may_add_secondary_hashkey(query.selectors, 'counter_name', cls.COUNTER_NAME_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp', [cls.COUNTER_NAME_TIMESTAMP_INDEX])
        result.set_query_filter(query)
        return result


class CaptureTable(TelemetryTable):
    TABLE_NAME = 'capture'
    FIELD_NAMES = ['capture_name', 'count', 'min', 'max', 'average', 'stddev']
    CAPTURE_NAME_TIMESTAMP_INDEX = 'index.capture_name-timestamp'

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=CaptureTable.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        capture_name_timestamp_index = GlobalAllIndex(CaptureTable.CAPTURE_NAME_TIMESTAMP_INDEX,
                                                      parts=[
                                                          HashKey('capture_name', data_type=STRING),
                                                          RangeKey('timestamp', data_type=NUMBER)
                                                      ],
                                                      throughput={
                                                          'read': 5,
                                                          'write': 5
                                                      })
        return cls.create(connection, global_secondary_indexes=[capture_name_timestamp_index])

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTable.should_handle(query)
        result.for_us = cls.is_for_us(query.selectors.keys())
        result.may_add_secondary_hashkey(query.selectors, 'counter_name', cls.CAPTURE_NAME_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp', [cls.CAPTURE_NAME_TIMESTAMP_INDEX])
        result.set_query_filter(query)
        return result


class SupportTable(TelemetryTable):
    TABLE_NAME = 'support'
    FIELD_NAMES = ['support']

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=SupportTable.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        return cls.create(connection, global_secondary_indexes=None)


class UiTable(TelemetryTable):
    TABLE_NAME = 'ui'
    FIELD_NAMES = ['ui_type', 'ui_object', 'ui_string', 'ui_integer']

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=UiTable.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        return cls.create(connection, global_secondary_indexes=None)
