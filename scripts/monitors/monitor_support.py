from AWS.query import Query
from AWS.selectors import SelectorEqual, SelectorGreaterThanEqual, SelectorLessThan
from monitor_base import Monitor
from misc.support import *
from misc.number_formatter import pretty_number
from misc.html_elements import *


class MonitorSupport(Monitor):
    def __init__(self, conn, start, end):
        Monitor.__init__(self, conn, 'support requests', start, end)

    def _query(self):
        query = Query()
        query.add('event_type', SelectorEqual('SUPPORT'))
        if self.start is not None:
            query.add('uploaded_at', SelectorGreaterThanEqual(self.start))
        if self.end is not None:
            query.add('uploaded_at', SelectorLessThan(self.end))

        self.events = self.query_all(query)[0]

    def _analyze(self):
        self.requests = Support.get_support_requests(self.events)

    def run(self):
        self._query()
        self._analyze()

    def report(self, summary):
        num_requests = len(self.requests)
        self.logger.info('# support requests: %d', num_requests)
        if num_requests == 0:
            return None
        summary.add_entry('Support requests', pretty_number(num_requests))

        table = Table()
        table.add_row(TableRow([TableHeader(Bold('Time (UTC)')),
                                TableHeader(Bold('Client Id')),
                                TableHeader(Bold('Contact Info')),
                                TableHeader(Bold('Message'))]))
        for request in self.requests:
            self.logger.info('\n' + request.display())
            match = re.match('(?P<date>.+)T(?P<time>.+)Z', request.timestamp)
            assert match
            table.add_row(TableRow([TableElement(Text(match.group('date') + ' ' + match.group('time'))),
                                    TableElement(Text(request.client)),
                                    TableElement(Text(request.contact_info)),
                                    TableElement(Text(request.message))]))

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

    def attachment(self):
        return None
