import Parse
from monitor_base import Monitor
from html_elements import *
from number_formatter import pretty_number


class MonitorCounters(Monitor):
    def __init__(self, conn, start, end):
        Monitor.__init__(self, conn=conn, desc='counters', start=start, end=end)
        self.events = list()
        self.clients = set()

    def _query(self):
        query = Parse.query.Query()
        query.add('event_type', Parse.query.SelectorEqual('COUNTER'))
        if self.start is not None:
            query.add('createdAt', Parse.query.SelectorGreaterThanEqual(self.start))
        if self.end is not None:
            query.add('createdAt', Parse.query.SelectorLessThan(self.end))

        self.events = self.query_all(query)[0]

    def _analyze(self):
        self.counters = dict()
        for event in self.events:
            self.clients.add(event['client'])
            name = event['counter_name']
            if name in self.counters:
                self.counters[name] += event['count']
            else:
                self.counters[name] = event['count']

    def run(self):
        print 'Querying %s...' % self.desc
        self._query()
        self._analyze()

    def report(self, summary):
        summary.add_entry('Counter event count', len(self.events))
        summary.add_entry('Counter type count', len(self.counters))
        summary.add_entry('Counter client count', len(self.clients))

        table = Table()
        table.add_row(TableRow([TableHeader(Bold('Name')),
                                TableHeader(Bold('Count'))]))
        for (name, count) in self.counters.items():
            table.add_row(TableRow([TableElement(Text(name)),
                                    TableElement(Text(pretty_number(count)), align='right')]))

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

    def attachment(self):
        return None