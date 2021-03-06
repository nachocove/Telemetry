from AWS.query import Query
from AWS.selectors import SelectorEqual
from monitor_base import Monitor
from misc.html_elements import *
from misc.number_formatter import pretty_number


class CounterInfo:
    """
    Represents all information / statistics about a particular counter.
    """
    def __init__(self, name):
        self.name = name
        self.clients = set()
        self.event_count = 0
        self.count = 0

    def add(self, client, count):
        self.clients.add(client)
        self.event_count += 1
        self.count += count


class MonitorCounters(Monitor):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'counters')
        Monitor.__init__(self, *args, **kwargs)
        self.events = list()
        self.counters = dict()
        self.client_count = 0

    def _query(self):
        query = Query()
        query.add('event_type', SelectorEqual('COUNTER'))
        query.add_range('uploaded_at', self.start, self.end)

        self.events = self.query_all(query)[0]

    def _analyze(self):
        self.counters = dict()
        for event in self.events:
            name = event['counter_name']
            client = event['client']
            count = event['count']
            if name not in self.counters:
                self.counters[name] = CounterInfo(name)
            self.counters[name].add(client, count)
        for info in self.counters.values():
            self.client_count += len(info.clients)

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        self._query()
        self._analyze()

    def report(self, summary, **kwargs):
        summary.add_entry('Counter event count', len(self.events))
        summary.add_entry('Counter type count', len(self.counters))
        summary.add_entry('Counter client count', self.client_count)

        if len(self.counters) == 0:
            return None

        table = Table()
        table.add_row(TableRow([TableHeader(Bold('Name')),
                                TableHeader(Bold('# clients')),
                                TableHeader(Bold('# events')),
                                TableHeader(Bold('Count'))]))
        for (name, info) in sorted(self.counters.items()):
            table.add_row(TableRow([TableElement(Text(name)),
                                    TableElement(Text(pretty_number(len(info.clients))), align='right'),
                                    TableElement(Text(pretty_number(info.event_count)), align='right'),
                                    TableElement(Text(pretty_number(info.count)), align='right')]))

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

    def attachment(self):
        return None