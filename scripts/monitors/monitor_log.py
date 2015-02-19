import pprint
import zipfile
from misc import event_formatter
import os
from AWS.query import Query
from AWS.selectors import SelectorEqual
from monitor_base import Monitor
from misc.number_formatter import pretty_number
from misc.html_elements import *
from logtrace import LogTrace
from analytics.token import TokenList, WhiteSpaceTokenizer
from analytics.cluster import Clusterer
from misc.threadpool import *


class MonitorLogTraceThread(ThreadPoolThread):
    def __init__(self, conn, logger):
        ThreadPoolThread.__init__(self)
        self.conn = conn
        self.logger = logger

    def start(self):
        self.conn = Monitor.run_with_retries(lambda: Monitor.clone_connection(self.conn), 'trace thread connection', 5)
        ThreadPoolThread.start(self)

    def process(self, trace):
        self.logger.debug('  [%d] Tracing client %s from %s to %s...', self.id, trace.client, trace.start, trace.end)

        def trace_query():
            trace.query(self.conn)
            return None

        def trace_query_exception():
            self.conn = Monitor.clone_connection(self.conn)
        Monitor.run_with_retries(trace_query, 'trace query', 5, trace_query_exception)


class MonitorLog(Monitor):
    def __init__(self, event_type=None, msg=None, rate_msg=None, *args, **kwargs):
        Monitor.__init__(self, *args, **kwargs)
        self.event_type = event_type
        self.events = list()  # events returned from the query
        self.report_ = dict()  # an analysis structure derived from raw events
        self.msg = msg  # message about total # of events in summary
        self.rate_msg = rate_msg  # message about the rate of the events in summary
        self.event_count = 0
        self.traces = list()  # list of event lists for every log event
        self.trace_enabled = False

    def _query(self):
        query = Query()
        query.add('event_type', SelectorEqual(self.event_type))
        query.add_range('uploaded_at', self.start, self.end)

        self.events, self.event_count = self.query_all(query)

    client_cache = {}
    def _get_device_info(self, client_id, start, end):
        key = "%s-%s-%s" % (client_id, start, end)
        if key not in self.client_cache:
            query = Query()
            query.add('client', SelectorEqual(client_id))
            query.add_range('timestamp', start, end)
            clients = Query.users(query, self.conn)
            # TODO Are these sorted by timestamp? Or do I need to sort them first?
            self.client_cache[key] = clients[-1] if clients else None
        return self.client_cache[key]

    def _classify(self):
        # Cluster log messages
        clusterer = Clusterer()
        tokenizer = WhiteSpaceTokenizer()
        for log in self.events:
            # We cluster using only the first line of the message
            message = log['message'].split('\n')[0]
            token_list = TokenList(tokenizer.process(message))
            clusterer.add(token_list)

        # Generate the report dictionary
        self.report_ = dict()
        for cluster in clusterer.clusters:
            self.report_[unicode(cluster.pattern)] = len(cluster)

    def _get_trace(self, event):
        assert 'client' in event and 'timestamp' in event
        (start, end) = LogTrace.get_time_window(event['timestamp'], 2, 0)
        trace = LogTrace(desc=self.desc, client=event['client'], start=start, end=end, prefix=self.prefix)
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
            self.traces.append(trace)
        self.logger.info('  Consolidate %d events into %d traces', len(self.events), len(self.traces))

        # Get all the traces
        num_threads = 2
        thread_pool = ThreadPool(num_threads, MonitorLogTraceThread, self.conn, self.logger)
        n = 0
        for trace in self.traces:
            thread_pool.threads[n].obj_queue.put(trace)
            n = (n + 1) % num_threads
        for n in range(num_threads):
            thread_pool.threads[n].obj_queue.put(None)
        thread_pool.start()
        thread_pool.wait()

    def run(self):
        self.logger.info('Querying %s...', self.desc)
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

    def report(self, summary, **kwargs):
        count = self.event_count
        rate = Monitor.compute_rate(count, self.start, self.end, 'hr')

        # Generate summary info
        self.logger.info('%s: %s', self.msg, pretty_number(count))
        self.logger.info('%s: %s', self.rate_msg, rate)
        summary.add_entry(self.msg, pretty_number(count))
        summary.add_entry(self.rate_msg, rate)

        # Create the monitor specific report if there is anything to report
        if count == 0:
            return None
        report = MonitorLog._process_report(self.report_)
        report_list = sorted(report.items(), key=lambda x: x[1], reverse=True)
        if kwargs.get('debug', False):
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
        if len(self.events) == 0:
            return None
        ef = event_formatter.RecordStyleEventFormatter(prefix=self.prefix)
        raw_log_prefix = '%s_%s' % (self.desc, self.end.file_suffix())
        raw_log_path = raw_log_prefix + '.txt'
        with open(raw_log_path, 'w') as raw_log:
            for event in self.events:
                (start, end) = LogTrace.get_time_window(event['uploaded_at'], 2, 0)
                client = self._get_device_info(event['client'], start, end)
                if client:
                    for k in client.keys():
                        if k not in ('build_number', 'build_version'):
                            continue
                        if k not in event:
                            event[k] = client[k]
                print >>raw_log, ef.format(event).encode('utf-8')
        zipped_log_path = raw_log_prefix + '.zip'
        zipped_file = zipfile.ZipFile(zipped_log_path, 'w', zipfile.ZIP_DEFLATED)
        zipped_file.write(raw_log_path)
        os.unlink(raw_log_path)

        for trace in self.traces:
            trace_path = trace.write_file()
            zipped_file.write(trace_path)
            os.unlink(trace_path)
        zipped_file.close()
        return zipped_log_path


class MonitorErrors(MonitorLog):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('event_type', 'ERROR')
        kwargs.setdefault('desc', 'errors')
        kwargs.setdefault('msg', 'Error count')
        kwargs.setdefault('rate_msg', 'ERROR rate')
        MonitorLog.__init__(self, *args, **kwargs)
        self.trace_enabled = True


class MonitorWarnings(MonitorLog):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('event_type', 'WARN')
        kwargs.setdefault('desc', 'warnings')
        kwargs.setdefault('msg', 'Warning count')
        kwargs.setdefault('rate_msg', 'Warning rate')
        MonitorLog.__init__(self, *args, **kwargs)
