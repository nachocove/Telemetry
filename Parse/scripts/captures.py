import Parse
import analytics


class Capture:
    def __init__(self, event):
        self.client = event['client']
        self.name = event['capture_name']
        self.statistics = analytics.statistics.Statistics(count=event['count'],
                                                          min_=event['min'],
                                                          max_=event['max'],
                                                          average=event['average'],
                                                          stddev=event['stddev'])
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
