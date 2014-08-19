import Parse
import analytics
from monitor_base import Monitor
from html_elements import *
from number_formatter import *


class Capture:
    def __init__(self, event):
        self.client = event['client']
        self.name = event['capture_name']
        count = float(event['count'])
        average = float(event['average'])
        first_moment = average * count
        second_moment = (float(event['stddev']) ** 2) + (average ** 2)
        self.statistics = analytics.statistics.Statistics(count=event['count'],
                                                          min_=event['min'],
                                                          max_=event['max'],
                                                          first_moment=first_moment,
                                                          second_moment=second_moment)
        self.timestamp = Parse.utc_datetime.UtcDateTime(event['timestamp']['iso'])

    def _same_client(self, other):
        if self.client != other.client:
            raise ValueError('cannot compare timestamp of different clients')

    def combine(self, other):
        if other > self:
            if other.statistics.count < self.statistics.count:
                self.statistics = self.statistics + other.statistics
            else:
                self.statistics = other.statistics

    def __lt__(self, other):
        self._same_client(other)
        return 0.0 > (self.timestamp - other.timestamp)

    def __gt__(self, other):
        self._same_client(other)
        return 0.0 < (self.timestamp - other.timestamp)

    def __eq__(self, other):
        self._same_client(other)
        return 0.0 == (self.timestamp - other.timestamp)

    def __le__(self, other):
        self._same_client(other)
        return 0.0 >= (self.timestamp - other.timestamp)

    def __ge__(self, other):
        self._same_client(other)
        return 0.0 <= (self.timestamp - other.timestamp)


class CaptureKind:
    def __init__(self, kind):
        self.kind = kind
        self.clients = dict()
        self.statistics = analytics.statistics.Statistics(count=0)

    def add(self, capture):
        if capture.client in self.clients:
            self.clients[capture.client].combine(capture)
        else:
            self.clients[capture.client] = capture

    def update_statistics(self):
        self.statistics = analytics.statistics.Statistics(count=0)
        for capture in self.clients.values():
            self.statistics += capture.statistics


class MonitorCaptures(Monitor):
    def __init__(self, conn, start, end):
        Monitor.__init__(self, conn=conn, desc='captures', start=start, end=end)
        self.events = []

    def _query(self):
        query = Parse.query.Query()
        query.add('event_type', Parse.query.SelectorEqual('CAPTURE'))
        if self.start is not None:
            query.add('createdAt', Parse.query.SelectorGreaterThanEqual(self.start))
        if self.end is not None:
            query.add('createdAt', Parse.query.SelectorLessThan(self.end))

        self.events = self.query_all(query)[0]

    def _analyze(self):
        self.captures = dict()
        for event in self.events:
            capture = Capture(event)
            if capture.name not in self.captures:
                self.captures[capture.name] = CaptureKind(capture.name)
            self.captures[capture.name].add(capture)
        self.clients = set()
        for capture_kind in self.captures.values():
            capture_kind.update_statistics()
            self.clients.update(capture_kind.clients)

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        self._query()
        self._analyze()

    def report(self, summary):
        summary.add_entry('Capture event count', pretty_number(len(self.events)))
        summary.add_entry('Capture kind count', pretty_number(len(self.captures)))
        summary.add_entry('Capture client count', pretty_number(len(self.clients)))

        if len(self.captures) == 0:
            return None

        table = Table()
        table.add_row(TableRow([TableHeader(Bold('Kind')),
                                TableHeader(Bold('# clients')),
                                TableHeader(Bold('Count')),
                                TableHeader(Bold('Min (msec)')),
                                TableHeader(Bold('Average (msec)')),
                                TableHeader(Bold('Max (msec)')),
                                TableHeader(Bold('Std.Dev. (msec)'))]))
        for kind in sorted(self.captures.keys()):
            capture_kind = self.captures[kind]
            stats = capture_kind.statistics
            if stats.count > 0:
                min_str = commafy('%.2f' % stats.min)
                avg_str = commafy('%.2f' % stats.mean())
                max_str = commafy('%.2f' % stats.max)
                sdev_str = commafy('%.2f' % stats.stddev())
            else:
                min_str = '-'
                avg_str = '-'
                max_str = '-'
                sdev_str = '-'
            table.add_row(TableRow([TableElement(Text(kind)),
                                    TableElement(Text(pretty_number(len(capture_kind.clients))), align='right'),
                                    TableElement(Text(pretty_number(stats.count)), align='right'),
                                    TableElement(Text(min_str), align='right'),
                                    TableElement(Text(avg_str), align='right'),
                                    TableElement(Text(max_str), align='right'),
                                    TableElement(Text(sdev_str), align='right')]))

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

    def attachment(self):
        return None