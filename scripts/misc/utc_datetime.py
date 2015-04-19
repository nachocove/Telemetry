import string
import datetime
import re

import dateutil.parser
import dateutil.tz
import pytz


class UtcDateTime:
    hours = ('h', 'hr', 'hrs')
    minutes = ('m', 'min', 'mins', 'minutes')
    seconds = ('s', 'sec', 'secs', 'seconds')
    days = ('d', 'day', 'days')
    match_str = "|".join(hours + minutes + seconds + days)

    def __init__(self, value=None):
        dt = None
        if isinstance(value, (str, unicode)):
            if value.startswith('now'):
                parts = value.split('-')
                dt = datetime.datetime.utcnow().replace(microsecond=0)
                if len(parts) == 1:
                    pass
                elif len(parts) == 2 and parts[1][0] in string.digits:
                    m = re.match(r'(?P<digit>[0-9]+)(?P<hmsd>[%s])' % self.match_str, parts[1])
                    if m:
                        if m.group('hmsd') in self.hours:
                            sub = datetime.timedelta(hours=int(m.group('digit')))
                        elif m.group('hmsd') in self.minutes:
                            sub = datetime.timedelta(minutes=int(m.group('digit')))
                        elif m.group('hmsd') in self.seconds:
                            sub = datetime.timedelta(seconds=int(m.group('digit')))
                        elif m.group('hmsd') in self.days:
                            sub = datetime.timedelta(days=int(m.group('digit')))
                        else:
                            raise Exception("unknown timeframe %s" % m.group('hmsd'))
                        dt -= sub
                else:
                    raise ValueError('format %s is not valid' % value)
            else:
                dt = dateutil.parser.parse(str(value))
        elif isinstance(value, UtcDateTime):
            dt = value.datetime
        elif isinstance(value, datetime.datetime):
            dt = value
        elif isinstance(value, int):
            milliseconds = value / 10000
            (days, milliseconds) = divmod(milliseconds, 86400 * 1000)
            date = datetime.date.fromordinal(days + 1)
            (hours, milliseconds) = divmod(milliseconds, 3600 * 1000)
            (minutes, milliseconds) = divmod(milliseconds, 60 * 1000)
            (seconds, milliseconds) = divmod(milliseconds, 1000)

            dt = datetime.datetime(year=date.year,
                                              month=date.month,
                                              day=date.day,
                                              hour=hours,
                                              minute=minutes,
                                              second=seconds,
                                              microsecond=milliseconds * 1000,
                                              )
        else:
            raise ValueError("Unsupported input type %s" % value.__class__)
        if dt:
            self.datetime = dt.replace(tzinfo=pytz.utc).astimezone(pytz.utc)

    def __repr__(self):
        s = self.datetime.strftime('%Y-%m-%dT%H:%M:%S')
        if self.datetime.microsecond == 0:
            return s + 'Z'
        return s + '.%03dZ' % int(self.datetime.microsecond / 1000.)

    def __cmp__(self, other):
        return cmp(self - other, 0.0)

    def __sub__(self, other):
        """
        Return the elapsed time in seconds (with millisecond resolution).
        """
        delta = self.datetime - other.datetime
        return (float(delta.days) * 86400.0) + float(delta.seconds) + (float(delta.microseconds) / 1.e6)

    def file_suffix(self):
        return str(self).replace(':', '_').replace('-', '_').replace('.', '_')

    def toticks(self):
        days = datetime.date.toordinal(self.datetime.date()) - 1
        ticks = days * 86400
        ticks += self.datetime.hour * 3600
        ticks += self.datetime.minute * 60
        ticks += self.datetime.second
        ticks = (ticks * 1000000) + UtcDateTime._round_to_millisecond(self.datetime.microsecond)
        return ticks * 10  # convert to ticks

    @staticmethod
    def _round_to_millisecond(microsecond):
        return microsecond - (microsecond % 1000)

    @staticmethod
    def now():
        dt = datetime.datetime.utcnow()
        return UtcDateTime(dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))

    @staticmethod
    def from_ticks(ticks):
        assert isinstance(ticks, int)
        return UtcDateTime(ticks)