from tables import TelemetryTable, LogTable, WbxmlTable, CounterTable, CaptureTable, SupportTable, UiTable
from boto.dynamodb2.items import Item
from misc.utc_datetime import UtcDateTime
from decimal import Decimal


class Event(Item):
    TABLE_CLASS = None

    def __init__(self, connection, id_, client, timestamp):
        self.table = type(self)._get_table(connection)
        Item.__init__(self, self.table)
        self['id'] = id_
        self['client'] = client
        self['timestamp'] = Event.parse_datetime(timestamp)

    @classmethod
    def _get_table(cls, connection):
        if connection is None:
            raise ValueError('no connection')
        if cls.TABLE_CLASS is None:
            raise ValueError('TABLE_CLASS is uninitialized')
        assert isinstance(cls.TABLE_CLASS, type)
        return cls.TABLE_CLASS(connection)

    def __getitem__(self, key):
        if key == 'timestamp' or key == u'timestamp':
            return UtcDateTime(Item.__getitem__(self, key))
        else:
            return Item.__getitem__(self, key)

    @classmethod
    def scan(cls, connection):
        results = cls._get_table(connection).scan()
        return [cls.from_item(connection, x) for x in results]


    @classmethod
    def from_item(cls, connection, item):
        kwargs = dict()
        for (key, value) in item.items():
            if key == u'id':
                kwargs[u'id_'] = value
            elif key == 'id':
                kwargs['id_'] = value
            else:
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


class LogEvent(Event):
    TABLE_CLASS = LogTable

    def __init__(self, connection, id_, client, timestamp, event_type, message):
        Event.__init__(self, connection, id_, client, timestamp)
        if event_type not in LogTable.EVENT_TYPES:
            raise ValueError('Unknown log event type %s' % event_type)
        self['event_type'] = event_type
        self['message'] = message


class WbxmlEvent(Event):
    TABLE_CLASS = WbxmlTable

    def __init__(self, connection, id_, client, timestamp, event_type, wbxml):
        Event.__init__(self, connection, id_, client, timestamp)
        if event_type not in WbxmlTable.EVENT_TYPES:
            raise ValueError('Unknown wbxml event type %s' % event_type)
        self['event_type'] = event_type
        self['wbxml'] = wbxml


class CounterEvent(Event):
    #TABLE_CLASS = CounterTable

    def __init__(self, connection, id_, client, timestamp, counter_name, count, counter_start, counter_end):
        Event.__init__(self, connection, id_, client, timestamp)
        self['counter_name'] = counter_name
        self['count'] = count
        if not isinstance(counter_start, UtcDateTime):
            raise TypeError('counter_start should be UtcDateTime')
        self['counter_start'] = counter_start.toticks()
        if not isinstance(counter_end, UtcDateTime):
            raise TypeError('counter_end should be UtcDateTime')
        self['counter_end'] = counter_end.toticks()


class CaptureEvent(Event):
    #TABLE_NAME = CaptureTable

    def __init__(self, connection, id_, client, timestamp, capture_name, count, min_, max_, average, stddev):
        Event.__init__(self, connection, id_, client, timestamp)
        self['capture_name'] = capture_name
        self['count'] = count
        self['min'] = min_
        self['max'] = max_
        self['average'] = average
        self['stddev'] = stddev


class SupportEvent(Event):
    #TABLE_NAME = SupportTable

    def __init__(self, connection, id_, client, timestamp, support):
        Event.__init__(self, connection, id_, client, timestamp)
        self['support'] = support


class UiEvent(Event):
    #TABLE_NAME = UiTable

    def __init__(self, connection, id_, client, timestamp, ui_type, ui_object, ui_string=None, ui_integer=None):
        Event.__init__(self, connection, id_, client, timestamp)
        self['ui_type'] = ui_type
        self['ui_object'] = ui_object
        if ui_string is not None:
            self['ui_string'] = ui_string
        if ui_integer is not None:
            self['ui_integer'] = ui_integer
