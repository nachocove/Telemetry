import Parse
from misc import event_formatter
from datetime import timedelta


class LogTrace:
    @staticmethod
    def get_time_window(utc, before, after):
        """
        Return two UtcDateTime objects that format time window that is
        'before' minutes before and 'after' minutes after datetime. Rounding
        to the nearest minute is applied to the datetime.
        """
        if isinstance(utc, str):
            start = Parse.utc_datetime.UtcDateTime(utc)
            end = Parse.utc_datetime.UtcDateTime(utc)
        else:
            start = Parse.utc_datetime.UtcDateTime(str(utc))
            end = Parse.utc_datetime.UtcDateTime(str(utc))
        start.datetime += timedelta(minutes=-before)
        end.datetime += timedelta(minutes=+after)

        def round_down_to_minutes(udt):
            udt.datetime -= timedelta(seconds=udt.datetime.second,
                                      microseconds=udt.datetime.microsecond)

        # Round down to the nearest minute for start time
        round_down_to_minutes(start)
        # Round up for end time
        if end.datetime.second or end.datetime.microsecond:
            round_down_to_minutes(end)
            end.datetime += timedelta(minutes=+1)

        return start, end

    def __init__(self, desc, client, start, end):
        self.desc = desc
        self.client = client
        self.start = start
        self.end = end
        self.events = []

    def __eq__(self, other):
        return (self.client == other.client) and (self.start == other.start) and (self.end == other.end)

    def __ne__(self, other):
        return not (self == other)

    def query(self, conn):
        query = Parse.query.Query()
        query.add('client', Parse.query.SelectorEqual(self.client))
        query.add('timestamp', Parse.query.SelectorGreaterThanEqual(self.start))
        query.add('timestamp', Parse.query.SelectorLessThan(self.end))
        self.events = Parse.query.Query.objects('Events', query, conn)[0]

    def _filename(self):
        return '%s.client_%s.%s.%s.trace.txt' % (self.desc, self.client,
                                                 self.start.file_suffix(), self.end.file_suffix())

    def write_file(self):
        ef = event_formatter.RecordStyleEventFormatter()
        fname = self._filename()
        with open(fname, 'w') as trace_file:
            for event in self.events:
                print >>trace_file, ef.format(event).encode('utf-8')
        return fname