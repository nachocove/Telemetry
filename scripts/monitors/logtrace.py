import time
from datetime import timedelta

from boto.dynamodb2.exceptions import ProvisionedThroughputExceededException
from AWS.query import Query
from AWS.selectors import SelectorEqual
from misc import event_formatter
from misc import utc_datetime


class LogTrace:
    @staticmethod
    def get_time_window(utc, before, after):
        """
        Return two UtcDateTime objects that format time window that is
        'before' minutes before and 'after' minutes after datetime. Rounding
        to the nearest minute is applied to the datetime.
        """
        if isinstance(utc, str):
            start = utc_datetime.UtcDateTime(utc)
            end = utc_datetime.UtcDateTime(utc)
        else:
            start = utc_datetime.UtcDateTime(str(utc))
            end = utc_datetime.UtcDateTime(str(utc))
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

    def __init__(self, desc, client, start, end, prefix):
        self.desc = desc
        self.client = client
        self.start = start
        self.end = end
        self.prefix = prefix
        self.events = []

    def __eq__(self, other):
        return (self.client == other.client) and (self.start == other.start) and (self.end == other.end)

    def __ne__(self, other):
        return not (self == other)

    def query(self, conn):
        query = Query()
        query.add('client', SelectorEqual(self.client))
        query.add_range('timestamp', self.start, self.end)
        done = False
        while not done:
            try:
                self.events = Query.events(query, conn)
                done = True
            except ProvisionedThroughputExceededException:
                time.sleep(5)

    def _filename(self):
        return '%s.client_%s.%s.%s.trace.txt' % (self.desc, self.client,
                                                 self.start.file_suffix(), self.end.file_suffix())

    def write_file(self):
        ef = event_formatter.RecordStyleEventFormatter(prefix=self.prefix)
        fname = self._filename()
        with open(fname, 'w') as trace_file:
            for event in self.events:
                print >>trace_file, ef.format(event).encode('utf-8')
        return fname