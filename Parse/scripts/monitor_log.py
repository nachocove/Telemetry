import Parse
import pprint
import zipfile
import event_formatter
from monitor_base import Monitor
from number_formatter import pretty_number
from html_elements import *


class MonitorLog(Monitor):
    def __init__(self, conn, event_type, desc, msg, rate_msg, start=None, end=None):
        Monitor.__init__(self, conn, desc, start, end)
        self.event_type = event_type
        self.events = list()  # events returned from the query
        self.report_ = dict()  # an analysis structure derived from raw events
        self.msg = msg  # message about total # of events in summary
        self.rate_msg = rate_msg  # message about the rate of the events in summary
        self.event_count = 0

    def _query(self):
        query = Parse.query.Query()
        query.add('event_type', Parse.query.SelectorEqual(self.event_type))
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
        results = Parse.query.Query.objects('Events', query, self.conn)[0]
        self.events.extend(results)
        while len(results) == query.limit and query.skip < 10000:
            query.skip += query.limit
            results = Parse.query.Query.objects('Events', query, self.conn)[0]
            self.events.extend(results)
        if self.event_count < len(self.events):
            self.event_count = len(self.events)

    def _classify(self):
        self.report_ = dict()
        for log in self.events:
            if log['message'] not in self.report_:
                self.report_[log['message']] = 1
            else:
                self.report_[log['message']] += 1

    def run(self):
        print 'Querying %s...' % self.desc
        self._query()
        self._classify()

    @staticmethod
    def _process_report(report):
        new_report = dict()
        for (message, count) in report.items():
            if len(message) > 70:
                new_message = message[:76] + ' ...'
            else:
                new_message = message
            new_report[new_message] = report[message]
        return new_report

    def report(self, summary):
        count = self.event_count
        rate = Monitor.compute_rate(count, self.start, self.end, 'hr')

        # Generate summary info
        print '%s: %s' % (self.msg, pretty_number(count))
        print '%s: %s' % (self.rate_msg, rate)
        summary.add_entry(self.msg, pretty_number(count))
        summary.add_entry(self.rate_msg, rate)

        # Create the monitor specific report if there is anything to report
        if count == 0:
            return None
        report = MonitorLog._process_report(self.report_)
        report_list = sorted(report.items(), key=lambda x: x[1], reverse=True)
        pprint.PrettyPrinter(depth=4).pprint(report_list)

        # Create paragraph with a header and table
        table = Table()
        row = TableRow([TableHeader(Bold('Count')),
                        TableHeader(Bold('Message'))])
        table.add_row(row)
        for (message, count) in report_list:
            row = TableRow([TableElement(Text(pretty_number(count))),
                            TableElement(Text(message))])
            table.add_row(row)
        if self.event_count > len(self.events):
            row = TableRow([TableElement(Text(pretty_number(self.event_count - len(self.events)))),
                            TableElement(Italic('Events skipped'))])
            table.add_row(row)

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

    def attachment(self):
        ef = event_formatter.RecordStyleEventFormatter()
        raw_log_path = '%s_%s.txt' % (self.desc, self.end.file_suffix())
        with open(raw_log_path, 'w') as raw_log:
            for event in self.events:
                print >>raw_log, ef.format(event)
        zipped_log_path = raw_log_path + '.zip'
        zipped_file = zipfile.ZipFile(zipped_log_path, 'w', zipfile.ZIP_DEFLATED)
        zipped_file.write(raw_log_path)
        zipped_file.close()
        return zipped_log_path


class MonitorErrors(MonitorLog):
    def __init__(self, conn, start=None, end=None):
        MonitorLog.__init__(self, conn, event_type='ERROR', desc='errors',
                            msg='Error count', rate_msg='Error rate',
                            start=start, end=end)


class MonitorWarnings(MonitorLog):
    def __init__(self, conn, start=None, end=None):
        MonitorLog.__init__(self, conn, event_type='WARN', desc='warnings',
                            msg='Warning count', rate_msg='Warning rate',
                            start=start, end=end)
