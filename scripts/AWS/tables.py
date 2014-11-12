from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex
from boto.dynamodb2.types import NUMBER, STRING
from table_query import TelemetryTableQuery
from misc.dict_formatter import DictFormatter
from selectors import SelectorEqual


class TelemetryTable(Table):
    PREFIX = None
    TABLE_NAME = None
    EVENT_TYPES = list()
    FIELD_NAMES = list()
    # These are fields that exist in all telemetry event tables
    # (but not in device info table)
    COMMON_FIELD_NAMES = ['id', 'client', 'timestamp', 'uploaded_at']
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
                                                    'read': 1,
                                                    'write': 1
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
    def has_field(cls, field):
        return field in TelemetryTable.COMMON_FIELD_NAMES or field in cls.FIELD_NAMES

    @classmethod
    def is_for_us(cls, selectors):
        for (field, sel) in selectors.items():
            if field == 'event_type' and len(sel) == 1 and isinstance(sel[0], SelectorEqual):
                if sel[0].value in cls.EVENT_TYPES:
                    continue
                else:
                    return False
            if not cls.has_field(field):
                return False
        return True

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTableQuery()
        result.for_us = cls.is_for_us(query.selectors)
        result.may_add_primary_hashkey(query.selectors, 'id')
        result.may_add_secondary_hashkey(query.selectors, 'client', TelemetryTable.CLIENT_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp',
                                          index_name=[
                                              TelemetryTable.CLIENT_TIMESTAMP_INDEX,
                                          ])
        result.set_query_filter(query, cls)
        return result

    def is_active(self):
        info = self.describe()
        return info[u'Table'][u'TableStatus'] == u'ACTIVE'

    @staticmethod
    def _format(info, field, indent):
        assert field in info
        return indent + field + ': ' + str(info[field]) + '\n'

    @staticmethod
    def _format_key_schema(index, formatter):
        schema = index[u'KeySchema']
        n = 0
        for attribute in schema:
            formatter.line(u'KeySchema[%d]: %s (%s)' % (n, attribute[u'AttributeName'], attribute[u'KeyType']))
            n += 1

    @staticmethod
    def _format_attributes(index, formatter):
        if u'AttributeDefinitions' not in index:
            return
        attributes = index[u'AttributeDefinitions']
        n = 0
        for attribute in attributes:
            formatter.line(u'AttributeDefinitons[%d]: %s (%s)' % (n, attribute[u'AttributeName'],
                                                                  attribute[u'AttributeType']))
            n += 1

    @staticmethod
    def _format_provisioned_throughput(index, formatter):
        formatter.push_dict(index[u'ProvisionedThroughput'])
        formatter.line('')
        formatter.field_dict(u'ReadCapacityUnits')
        formatter.field_dict(u'WriteCapacityUnits')
        formatter.field_dict(u'NumberOfDecreasesToday')
        formatter.pop_dict()

    @staticmethod
    def _format_projection(index, formatter):
        throughput = index.get(u'Projection', None)
        if throughput is None:
            return
        formatter.push_dict(throughput)
        formatter.field_dict(u'ProjectionType')
        formatter.pop_dict()

    @staticmethod
    def _format_index(states_only, index, prefix, prefix_desc, formatter):
        assert prefix in [u'Table', u'Index']

        formatter.line(u'[%s: %s]' % (prefix_desc, index[prefix + u'Name']))
        formatter.push_dict(index)
        formatter.field_dict(prefix + u'Status')
        formatter.field_dict(u'ItemCount')
        formatter.field_dict(prefix + u'SizeBytes')
        formatter.pop_dict()
        if states_only:
            return formatter.output

        TelemetryTable._format_key_schema(index, formatter)
        TelemetryTable._format_attributes(index, formatter)
        TelemetryTable._format_projection(index, formatter)
        TelemetryTable._format_provisioned_throughput(index, formatter)
        formatter.line('')
        return formatter.output

    def __str__(self):
        return self.display(False)

    def display(self, states_only):
        info = self.describe()[u'Table']
        formatter = DictFormatter()
        TelemetryTable._format_index(states_only, info, u'Table', u'Table', formatter)

        # Global Indexes
        if u'GlobalSecondaryIndexes' in info:
            formatter.increase_indent()
            for index in info[u'GlobalSecondaryIndexes']:
                TelemetryTable._format_index(states_only, index, u'Index', u'GlobalSecondaryIndex', formatter)
            formatter.decrease_indent()

        # Local Indexes
        if u'LocalSecondaryIndexes' in info:
            formatter.increase_indent()
            for index in info[u'LocalSecondaryIndexes']:
                TelemetryTable._format_index(states_only, index, u'Index', u'LocalSecondaryIndex', formatter)
            formatter.decrease_indent()

        return formatter.output

    @staticmethod
    def find_table_class(full_table_name):
        for cls in TABLE_CLASSES:
            if full_table_name.endswith(u'.telemetry.' + cls.TABLE_NAME):
                return cls
        return TelemetryTable


class DeviceInfoTable(TelemetryTable):
    TABLE_NAME = 'device_info'
    FIELDS_NAMES = ['os_type', 'os_version', 'device_model', 'build_version', 'build_number']

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=DeviceInfoTable.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        return cls.create(connection, global_secondary_indexes=None)


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
                                                        'read': 1,
                                                        'write': 1
                                                    })
        return cls.create(connection, global_secondary_indexes=[event_type_timestamp_index])

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTable.should_handle(query)
        result.for_us = cls.is_for_us(query.selectors)
        result.may_add_secondary_hashkey(query.selectors, 'event_type', cls.EVENT_TYPE_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp', [cls.EVENT_TYPE_TIMESTAMP_INDEX])
        result.set_query_filter(query, cls)
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
                                                        'read': 1,
                                                        'write': 1
                                                    })
        return cls.create(connection, global_secondary_indexes=[event_type_timestamp_index])

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTable.should_handle(query)
        result.for_us = cls.is_for_us(query.selectors)
        result.may_add_secondary_hashkey(query.selectors, 'event_type', cls.EVENT_TYPE_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp', [cls.EVENT_TYPE_TIMESTAMP_INDEX])
        result.set_query_filter(query, cls)
        return result


class CounterTable(TelemetryTable):
    TABLE_NAME = 'counter'
    EVENT_TYPES = ['COUNTER']
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
                                                          'read': 1,
                                                          'write': 1
                                                      })
        return cls.create(connection, global_secondary_indexes=[counter_name_timestamp_index])

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTable.should_handle(query)
        result.for_us = cls.is_for_us(query.selectors)
        result.may_add_secondary_hashkey(query.selectors, 'counter_name', cls.COUNTER_NAME_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp', [cls.COUNTER_NAME_TIMESTAMP_INDEX])
        result.set_query_filter(query, cls)
        return result


class CaptureTable(TelemetryTable):
    TABLE_NAME = 'capture'
    EVENT_TYPES = ['CAPTURE']
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
                                                          'read': 1,
                                                          'write': 1
                                                      })
        return cls.create(connection, global_secondary_indexes=[capture_name_timestamp_index])

    @classmethod
    def should_handle(cls, query):
        result = TelemetryTable.should_handle(query)
        result.for_us = cls.is_for_us(query.selectors)
        result.may_add_secondary_hashkey(query.selectors, 'counter_name', cls.CAPTURE_NAME_TIMESTAMP_INDEX)
        result.may_add_secondary_rangekey(query.selectors, 'timestamp', [cls.CAPTURE_NAME_TIMESTAMP_INDEX])
        result.set_query_filter(query, cls)
        return result


class SupportTable(TelemetryTable):
    TABLE_NAME = 'support'
    EVENT_TYPES = ['SUPPORT']
    FIELD_NAMES = ['support']

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=SupportTable.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        return cls.create(connection, global_secondary_indexes=None)


class UiTable(TelemetryTable):
    TABLE_NAME = 'ui'
    EVENT_TYPES = ['UI']
    FIELD_NAMES = ['ui_type', 'ui_object', 'ui_string', 'ui_integer']

    def __init__(self, connection):
        TelemetryTable.__init__(self, connection=connection, table_name=UiTable.TABLE_NAME)

    @classmethod
    def create_table(cls, connection):
        return cls.create(connection, global_secondary_indexes=None)

TABLE_CLASSES = [
    DeviceInfoTable,
    LogTable,
    WbxmlTable,
    CounterTable,
    CaptureTable,
    SupportTable,
    UiTable
]