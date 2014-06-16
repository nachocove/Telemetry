import dateutil.parser
import dateutil.tz
import datetime


class UtcDateTime:
    def __init__(self, iso8601):
        self.datetime = dateutil.parser.parse(iso8601)

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

    @staticmethod
    def now():
        dt = datetime.datetime.utcnow()
        return UtcDateTime(dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
