import Parse
from monitor_base import Monitor
from html_elements import *
from captures import Capture, CaptureKind
from number_formatter import *


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
        query.limit = 0
        query.count = 1
        self.event_count = Parse.query.Query.objects('Events', query, self.conn)[1]

        query.limit = 1000
        query.skip = 0

        # Keep querying until the list is less than 1000
        # TODO - we need a robust way to pull more than 11,000 events
        results = Parse.query.Query.objects('Events', query, self.conn)[0]
        self.events.extend(results)
        while len(results) == query.limit and query.skip < 10000:
            query.skip += query.limit
            results = Parse.query.Query.objects('Events', query, self.conn)[0]
            self.events.extend(results)
        if self.event_count < len(self.events):
            self.event_count = len(self.events)

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
        print 'Query %s...' % self.desc
        self._query()
        self._analyze()

    def report(self, summary):
        summary.add_entry('Capture event count', pretty_number(len(self.events)))
        summary.add_entry('Capture kind count', pretty_number(len(self.captures)))
        summary.add_entry('Capture client count', pretty_number(len(self.clients)))

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
                avg_str = commafy('%.2f' % stats.average)
                max_str = commafy('%.2f' % stats.max)
                sdev_str = commafy('%.2f' % stats.stddev)
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