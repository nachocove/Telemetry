# Copyright 2014, NachoCove, Inc
from datetime import datetime
from misc.utc_datetime import UtcDateTime


def iso_z_format(date):
    raw = date.isoformat()
    keep = raw.split('+', 1)[0]
    if date.microsecond == 0:
        return keep + '.000Z'
    return keep[:-3] + 'Z'

def json_formatter(obj):
    if isinstance(obj, UtcDateTime):
        return obj.datetime.isoformat('T')
    elif isinstance(obj, datetime):
        return obj.isoformat('T')
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))

