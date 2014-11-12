from tables import LogTable, WbxmlTable, CounterTable, CaptureTable, SupportTable, UiTable
from boto.dynamodb2.items import Item
from misc.utc_datetime import UtcDateTime
from decimal import Decimal


class Event(Item):
    # All derived classes must set this to the companion telemetry table class.
    TABLE_CLASS = None
    # Some fields happen to be python reserved words. For those fields,
    # we append a trailing '_' to the input parameter of the class constructor.
    # But in DynamoDB, we still keep the original name. So, some special processing
    # must be applied to those fields. CONFLICT_FIELDS is a list of these fields.
    # 'id' is the default conflict field and does not need to be added into
    # CONFLICT_FIELDS.
    CONFLICT_FIELDS = []

    def __init__(self, connection, id_, client, timestamp, uploaded_at):
        self.table = type(self)._get_table(connection)
        Item.__init__(self, self.table)
        self['id'] = id_
        self['client'] = client
        self['timestamp'] = Event.parse_datetime(timestamp)
        self['uploaded_at'] = Event.parse_datetime(uploaded_at)

    # Some field requires a translation between human-readable format to the
    # format used in DynamoDb. Translation from humna-readable format to DynamoDB
    # format is done in the constructors. The other direction is done by overloading
    # __getitem__ and handle them as exception cases
    def __getitem__(self, key):
        if key == 'event_type':
            # For tables that optimize away event_type, we need to recreate it
            table_cls = self.__class__.TABLE_CLASS
            if len(table_cls.EVENT_TYPES) == 1:
                return table_cls.EVENT_TYPES[0]
        if key in ['timestamp', 'uploaded_at']:
            return UtcDateTime(Item.__getitem__(self, key))
        else:
            return Item.__getitem__(self, key)

    def __str__(self):
        s = self._header_str()
        for field in sorted(self.keys()):
            if field in ['id', 'client', 'timestamp', 'uploaded_at']:
                continue
            s += '\n%s: %s' % (field, self[field])
        return s

    def _header_str(self):
        s = 'id: %s' % self['id']
        s += '\nclient: %s' % self['client']
        s += '\ntimestamp: %s' % self['timestamp']
        s += '\nuploaded_at: %s' % self['uploaded_at']
        return s

    def _field_str(self, field):
        return '\n%s: %s' % (field, str(self[field]))

    def items(self):
        retval = list()
        for (field, value) in Item.items(self):
            retval.append((field, self[field]))
        return retval

    @classmethod
    def _get_table(cls, connection):
        if connection is None:
            raise ValueError('no connection')
        if cls.TABLE_CLASS is None:
            raise ValueError('TABLE_CLASS is uninitialized')
        assert isinstance(cls.TABLE_CLASS, type)
        return cls.TABLE_CLASS(connection)

    @classmethod
    def scan(cls, connection):
        results = cls._get_table(connection).scan()
        return cls.from_db_results(connection, results)

    @classmethod
    def from_item(cls, connection, item):
        kwargs = dict()
        for (key, value) in item.items():
            # id is a reserve word. So, all our constructor use id_ instead
            if key == 'id' or key in cls.CONFLICT_FIELDS:
                key += '_'
            kwargs[key] = value
        return cls(connection, **kwargs)

    @staticmethod
    def parse_datetime(timestamp):
        if isinstance(timestamp, int):
            return timestamp
        elif isinstance(timestamp, Decimal):
            return int(timestamp)
        elif isinstance(timestamp, UtcDateTime):
            return timestamp.toticks()
        else:
            raise TypeError('timestamp should be UtcDateTime or int')

    @classmethod
    def from_db_results(cls, connection, results):
        return [cls.from_item(connection, r) for r in results]

    @staticmethod
    def sort_chronologically(objects):
        return sorted(objects, key=lambda x: x['timestamp'])


class LogEvent(Event):
    TABLE_CLASS = LogTable

    def __init__(self, connection, id_, client, timestamp, uploaded_at, event_type, message):
        Event.__init__(self, connection, id_, client, timestamp, uploaded_at)
        if event_type not in LogTable.EVENT_TYPES:
            raise ValueError('Unknown log event type %s' % event_type)
        self['event_type'] = event_type
        self['message'] = message

    def __str__(self):
        return self._header_str() + self._field_str('message')


class WbxmlEvent(Event):
    TABLE_CLASS = WbxmlTable

    def __init__(self, connection, id_, client, timestamp, uploaded_at, event_type, wbxml):
        Event.__init__(self, connection, id_, client, timestamp, uploaded_at)
        if event_type not in WbxmlTable.EVENT_TYPES:
            raise ValueError('Unknown wbxml event type %s' % event_type)
        self['event_type'] = event_type
        self['wbxml'] = wbxml

    def __str__(self):
        return self._header_str() + self._field_str('wbxml')


class CounterEvent(Event):
    TABLE_CLASS = CounterTable

    def __init__(self, connection, id_, client, timestamp, uploaded_at,
                 counter_name, count, counter_start, counter_end):
        Event.__init__(self, connection, id_, client, timestamp, uploaded_at)
        self['counter_name'] = counter_name
        self['count'] = int(count)
        self['counter_start'] = Event.parse_datetime(counter_start)
        self['counter_end'] = Event.parse_datetime(counter_end)

    def __getitem__(self, key):
        if key in ['counter_start', u'counter_start', 'counter_end', u'counter_start']:
            return UtcDateTime(Item.__getitem__(self, key))
        else:
            return Event.__getitem__(self, key)

    def __str__(self):
        s = self._header_str()
        s += self._field_str('counter_name')
        s += self._field_str('count')
        s += self._field_str('counter_start')
        s += self._field_str('counter_end')
        return s


class CaptureEvent(Event):
    TABLE_CLASS = CaptureTable
    CONFLICT_FIELDS = ['min', 'max', 'sum']

    def __init__(self, connection, id_, client, timestamp, uploaded_at,
                 capture_name, count, min_, max_, sum_, sum2):
        Event.__init__(self, connection, id_, client, timestamp, uploaded_at)
        self['capture_name'] = capture_name
        # Convert all Decimal to int
        self['count'] = int(count)
        self['min'] = int(min_)
        self['max'] = int(max_)
        self['sum'] = int(sum_)
        self['sum2'] = int(sum2)

    def __str__(self):
        s = self._header_str()
        s += self._field_str('capture_name')
        s += self._field_str('count')
        s += self._field_str('min')
        s += self._field_str('max')
        s += self._field_str('sum')
        s += self._field_str('sum2')
        return s


class SupportEvent(Event):
    TABLE_CLASS = SupportTable

    def __init__(self, connection, id_, client, timestamp, uploaded_at, support):
        Event.__init__(self, connection, id_, client, timestamp, uploaded_at)
        self['support'] = support

    def __str__(self):
        return self._header_str() + self._field_str('support')


class UiEvent(Event):
    TABLE_CLASS = UiTable

    def __init__(self, connection, id_, client, timestamp, uploaded_at,
                 ui_type, ui_object, ui_string=None, ui_integer=None):
        Event.__init__(self, connection, id_, client, timestamp, uploaded_at)
        self['ui_type'] = ui_type
        self['ui_object'] = ui_object
        if ui_string is not None:
            self['ui_string'] = ui_string
        if ui_integer is not None:
            self['ui_integer'] = ui_integer

    def __str__(self):
        s = self._header_str()
        s += self._field_str('ui_type')
        s += self._field_str('ui_object')
        keys = self.keys()
        if 'ui_string' in keys:
            s += self._field_str('ui_string')
        if 'ui_integer' in keys:
            s += self._field_str('ui_integer')
        return s
