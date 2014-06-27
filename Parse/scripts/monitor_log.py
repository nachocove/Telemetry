import Parse
import pprint
import zipfile
import event_formatter
from datetime import timedelta
from monitor_base import Monitor
from number_formatter import pretty_number
from html_elements import *
from logtrace import LogTrace


class MonitorLog(Monitor):
    def __init__(self, conn, event_type, desc, msg, rate_msg, start=None, end=None):
        Monitor.__init__(self, conn, desc, start, end)
        self.event_type = event_type
        self.events = list()  # events returned from the query
        self.report_ = dict()  # an analysis structure derived from raw events
        self.msg = msg  # message about total # of events in summary
        self.rate_msg = rate_msg  # message about the rate of the events in summary
        self.event_count = 0
        self.traces = list()  # list of event lists for every log event
        self.trace_enabled = False

    def _query(self):
        query = Parse.query.Query()
        query.add('event_type', Parse.query.SelectorEqual(self.event_type))
        if self.start is not None:
            query.add('createdAt', Parse.query.SelectorGreaterThanEqual(self.start))
        if self.end is not None:
            query.add('createdAt', Parse.query.SelectorLessThan(self.end))

        self.events, self.event_count = self.query_all(query)

    def _classify(self):
        self.report_ = dict()
        for log in self.events:
            if log['message'] not in self.report_:
                self.report_[log['message']] = 1
            else:
                self.report_[log['message']] += 1

    def _get_trace(self, event):
        assert 'client' in event and 'timestamp' in event
        (start, end) = LogTrace.get_time_window(event['timestamp']['iso'], 2, 0)
        trace = LogTrace(desc=self.desc, client=event['client'], start=start, end=end)
        return trace

    def _get_traces(self):
        self.traces = list()
        clients = dict()
        # Examine all events and consolidate traces when it makes sense
        for event in self.events:
            trace = self._get_trace(event)
            client = event['client']
            if client not in clients:
                clients[client] = trace
            else:
                if clients[client] == trace:
                    # with rounding of the time window to the nearest
                    # minutes, if it is the same as another trace,
                    # consolidate them.
                    continue
                clients[client] = trace
            print '  Tracing client %s at %s...' % (event['client'], event['timestamp']['iso'])
            self.traces.append(trace)
        # Get all the traces
        for trace in self.traces:
            trace.query(self.conn)

    def run(self):
        print 'Querying %s...' % self.desc
        self._query()
        self._classify()
        if self.trace_enabled:
            self._get_traces()

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
        raw_log_prefix = '%s_%s' % (self.desc, self.end.file_suffix())
        raw_log_path = raw_log_prefix + '.txt'
        with open(raw_log_path, 'w') as raw_log:
            for event in self.events:
                print >>raw_log, ef.format(event).encode('utf-8')
        zipped_log_path = raw_log_prefix + '.zip'
        zipped_file = zipfile.ZipFile(zipped_log_path, 'w', zipfile.ZIP_DEFLATED)
        zipped_file.write(raw_log_path)

        for trace in self.traces:
            fname = trace.write_file()
            zipped_file.write(fname)
        zipped_file.close()
        return zipped_log_path


class MonitorErrors(MonitorLog):
    def __init__(self, conn, start=None, end=None):
        MonitorLog.__init__(self, conn, event_type='ERROR', desc='errors',
                            msg='Error count', rate_msg='Error rate',
                            start=start, end=end)
        self.trace_enabled = True


class MonitorWarnings(MonitorLog):
    def __init__(self, conn, start=None, end=None):
        MonitorLog.__init__(self, conn, event_type='WARN', desc='warnings',
                            msg='Warning count', rate_msg='Warning rate',
                            start=start, end=end)
