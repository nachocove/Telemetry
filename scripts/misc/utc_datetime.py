import dateutil.parser
import dateutil.tz
import datetime
import pytz


class UtcDateTime:
    def __init__(self, value=None):
        if isinstance(value, str) or isinstance(value, unicode):
            self.datetime = dateutil.parser.parse(str(value))
        elif isinstance(value, datetime.datetime):
            self.datetime = value
        elif isinstance(value, int):
            milliseconds = value / 10000
            (days, milliseconds) = divmod(milliseconds, 86400 * 1000)
            date = datetime.date.fromordinal(days + 1)
            (hours, milliseconds) = divmod(milliseconds, 3600 * 1000)
            (minutes, milliseconds) = divmod(milliseconds, 60 * 1000)
            (seconds, milliseconds) = divmod(milliseconds, 1000)

            self.datetime = datetime.datetime(year=date.year,
                                              month=date.month,
                                              day=date.day,
                                              hour=hours,
                                              minute=minutes,
                                              second=seconds,
                                              microsecond=milliseconds * 1000,
                                              tzinfo=pytz.utc)

    def __repr__(self):
        s = self.datetime.strftime('%Y-%m-%dT%H:%M:%S')
        if self.datetime.microsecond == 0:
            return s + 'Z'
        return s + '.%03dZ' % int(self.datetime.microsecond/1000.)

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