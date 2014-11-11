from AWS.selectors import SelectorEqual
from AWS.query import Query
from AWS.events import Event
from monitor_base import Monitor
from viewcontroller import ViewControllerSet
from misc.number_formatter import *
from misc.html_elements import *
from misc.utc_datetime import UtcDateTime


class MonitorUi(Monitor):
    def __init__(self, conn, start, end):
        Monitor.__init__(self, conn=conn, desc='UI', start=start, end=end)
        self.events = []
        self.view_controller_sets = dict()
        self.num_vc_events = 0
        self.clients = set()

    def _query(self):
        query = Query()
        query.add('event_type', SelectorEqual('UI'))
        query.add_range('uploaded_at', self.start, self.end)

        self.events = Event.sort_chronologically(self.query_all(query)[0])

    def _analyze(self):
        self.view_controller_sets = dict()
        self.num_vc_events = 0
        self.clients = set()
        for event in self.events:
            assert event['event_type'] == 'UI'
            if event['ui_type'] != 'UIViewController':
                continue  # TODO - add analyze of non-view controller later
            self.num_vc_events += 1
            self.clients.add(event['client'])
            vc_type = event['ui_object']
            client = event['client']
            if vc_type not in self.view_controller_sets:
                self.view_controller_sets[vc_type] = ViewControllerSet(description=vc_type)
            vc = self.view_controller_sets[vc_type].get(client)
            timestamp = UtcDateTime(event['timestamp']['iso'])
            vc.parse(timestamp, event['ui_string'])
        for (vc_type, vc_set) in self.view_controller_sets.items():
            vc_set.aggregate_samples()

    def run(self):
        self._query()
        self._analyze()

    @staticmethod
    def _report_one_transition_row(table, vc_type, vc_event, stats):
        if stats.count > 0:
            # Multiple by 1000 to convert to msec
            min_str = commafy('%.2f' % (1000.0 * stats.min))
            avg_str = commafy('%.2f' % (1000.0 * stats.mean()))
            max_str = commafy('%.2f' % (1000.0 * stats.max))
            sdev_str = commafy('%.2f' % (1000.0 * stats.stddev()))
        else:
            min_str = '-'
            avg_str = '-'
            max_str = '-'
            sdev_str = '-'
        row_elements = [TableElement(Text(vc_event)),
                        TableElement(Text(pretty_number(stats.count)), align='right'),
                        TableElement(Text(min_str), align='right'),
                        TableElement(Text(avg_str), align='right'),
                        TableElement(Text(max_str), align='right'),
                        TableElement(Text(sdev_str), align='right')]
        if vc_type is not None:
            row_elements = [TableElement(Text(vc_type), rowspan=4, valign='top')] + row_elements
        table.add_row(TableRow(row_elements), advance_color=False)

    @staticmethod
    def _report_one_inuse_row(table, vc_type, stats):
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
        table.add_row(TableRow([TableElement(Text(vc_type)),
                                TableElement(Text(pretty_number(stats.count)), align='right'),
                                TableElement(Text(min_str), align='right'),
                                TableElement(Text(avg_str), align='right'),
                                TableElement(Text(max_str), align='right'),
                                TableElement(Text(sdev_str), align='right')]))

    def report(self, summary):
        summary.add_entry('UI event count', pretty_number(len(self.events)))
        summary.add_entry('UI view controller event count', pretty_number(self.num_vc_events))
        summary.add_entry('UI client count', pretty_number(len(self.clients)))

        table_transition = Table()
        table_transition.color_list.configure(colors=[None, '#faffff'])
        table_transition.add_row(TableRow([TableHeader(Bold('View Controller')),
                                           TableHeader(Bold('Event')),
                                           TableHeader(Bold('Count')),
                                           TableHeader(Bold('Min. (msec)')),
                                           TableHeader(Bold('Average (msec)')),
                                           TableHeader(Bold('Max. (msec)')),
                                           TableHeader(Bold('Std.Dev. (msec)'))]))
        for vc_type in sorted(self.view_controller_sets.keys()):
            vc = self.view_controller_sets[vc_type]
            for vc_event in ('WILL_APPEAR', 'DID_APPEAR', 'WILL_DISAPPEAR', 'DID_DISAPPEAR'):
                stats = vc.samples[vc_event].statistics
                if vc_event == 'WILL_APPEAR':
                    tmp_vc_type = vc_type
                else:
                    tmp_vc_type = None
                self._report_one_transition_row(table_transition, tmp_vc_type, vc_event, stats)
            table_transition.color_list.advance()

        table_usage = Table()
        table_usage.color_list.configure(colors=[None, '#faffff'])
        table_usage.add_row(TableRow([TableHeader(Bold('View Controller')),
                                      TableHeader(Bold('Count')),
                                      TableHeader(Bold('Min. (sec)')),
                                      TableHeader(Bold('Average (sec)')),
                                      TableHeader(Bold('Max. (sec)')),
                                      TableHeader(Bold('Std.Dev. (sec)'))]))
        for vc_type in sorted(self.view_controller_sets.keys()):
            stats = self.view_controller_sets[vc_type].samples['IN_USE'].statistics
            self._report_one_inuse_row(table_usage, vc_type, stats)

        paragraphs = [Paragraph([Bold('UI View Controller Usage'),
                                 table_usage]),
                      Paragraph([Bold('UI View Controller Transition'),
                                 table_transition])]
        return paragraphs

    def attachment(self):
        return None