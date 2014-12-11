from AWS.query import Query
from AWS.selectors import SelectorEqual
from monitor_base import Monitor
from misc.support import *
from misc.number_formatter import pretty_number
from misc.html_elements import *


class MonitorSupport(Monitor):
    def __init__(self, prefix=None, *args, **kwargs):
        kwargs.setdefault('desc', 'support requests')
        Monitor.__init__(self, *args, **kwargs)
        self.prefix = prefix
        self.telemetry_viewer_url_prefix = 'http://localhost:8000/'

    def _query(self):
        query = Query()
        query.add('event_type', SelectorEqual('SUPPORT'))
        query.add_range('uploaded_at', self.start, self.end)

        self.events = self.query_all(query)[0]

    def _analyze(self):
        self.requests = Support.get_support_requests(self.events)

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        self._query()
        self._analyze()

    def report(self, summary, **kwargs):
        num_requests = len(self.requests)
        self.logger.info('# support requests: %d', num_requests)
        if num_requests == 0:
            return None
        summary.add_entry('Support requests', pretty_number(num_requests))

        table = Table()
        table.add_row(TableRow([TableHeader(Bold('Time (UTC)')),
                                TableHeader(Bold('Client Id')),
                                TableHeader(Bold('Contact Info')),
                                TableHeader(Bold('Message')),
                                TableHeader(Bold(''))]))
        for request in self.requests:
            self.logger.info('\n' + request.display())
            match = re.match('(?P<date>.+)T(?P<time>.+)Z', request.timestamp)
            assert match
            table.add_row(TableRow([TableElement(Text(match.group('date') + ' ' + match.group('time'))),
                                    TableElement(Text(request.client)),
                                    TableElement(Text(request.contact_info)),
                                    TableElement(Text(request.message)),
                                    TableElement(Link("%s Telemetry link" % self.prefix.capitalize(), '%sbugfix/%s/logs/%s/%s/1/' % (self.telemetry_viewer_url_prefix, self.prefix, request.client, request.timestamp)))]))

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

    def attachment(self):
        return None
