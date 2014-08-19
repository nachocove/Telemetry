import Parse
from monitor_base import Monitor
from viewcontroller import ViewController
from number_formatter import *
from html_elements import *


class MonitorUi(Monitor):
    def __init__(self, conn, start, end):
        Monitor.__init__(self, conn=conn, desc='UI', start=start, end=end)
        self.events = []
        self.view_controllers = dict()
        self.num_vc_events = 0
        self.clients = set()

    def _query(self):
        query = Parse.query.Query()
        query.add('event_type', Parse.query.SelectorEqual('UI'))
        if self.start is not None:
            query.add('createdAt', Parse.query.SelectorGreaterThanEqual(self.start))
        if self.end is not None:
            query.add('createdAt', Parse.query.SelectorLessThan(self.end))

        self.events = self.query_all(query)[0]

    def _analyze(self):
        self.view_controllers = dict()
        self.num_vc_events = 0
        self.clients = set()
        for event in self.events:
            assert event['event_type'] == 'UI'
            if event['ui_type'] != 'UIViewController':
                continue  # TODO - add analyze of non-view controller later
            self.num_vc_events += 1
            self.clients.add(event['client'])
            vc_type = event['ui_object']
            if vc_type not in self.view_controllers:
                vc = ViewController(description=vc_type)
                self.view_controllers[vc_type] = vc
            else:
                vc = self.view_controllers[vc_type]
            timestamp = Parse.utc_datetime.UtcDateTime(event['timestamp']['iso'])
            vc.parse(timestamp, event['ui_string'])

    def run(self):
        self._query()
        self._analyze()

    @staticmethod
    def _report_one_transition_row(table, vc_type, vc_event, stats):
        if stats.count > 0:
            min_str = commafy('%.4f' % stats.min)
            avg_str = commafy('%.4f' % stats.mean())
            max_str = commafy('%.4f' % stats.max)
            sdev_str = commafy('%.4f' % stats.stddev())
        else:
            min_str = '-'
            avg_str = '-'
            max_str = '-'
            sdev_str = '-'
        table.add_row(TableRow([TableElement(Text(vc_type)),
                                TableElement(Text(vc_event)),
                                TableElement(Text(pretty_number(stats.count)), align='right'),
                                TableElement(Text(min_str), align='right'),
                                TableElement(Text(avg_str), align='right'),
                                TableElement(Text(max_str), align='right'),
                                TableElement(Text(sdev_str), align='right')]))

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
        table_transition.add_row(TableRow([TableHeader(Bold('View Controller')),
                                           TableHeader(Bold('Event')),
                                           TableHeader(Bold('Count')),
                                           TableHeader(Bold('Min (msec)')),
                                           TableHeader(Bold('Average (msec)')),
                                           TableHeader(Bold('Max (msec)')),
                                           TableHeader(Bold('Std.Dev. (msec)'))]))
        for vc_type in sorted(self.view_controllers.keys()):
            vc = self.view_controllers[vc_type]
            for vc_event in ('WILL_APPEAR', 'DID_APPEAR', 'WILL_DISAPPEAR', 'DID_DISAPPEAR'):
                stats = vc.samples[vc_event].statistics
                self._report_one_transition_row(table_transition, vc_type, vc_event, stats)

        table_usage = Table()
        table_usage.add_row(TableRow([TableHeader(Bold('View Controller')),
                                      TableHeader(Bold('Count')),
                                      TableHeader(Bold('Min (msec)')),
                                      TableHeader(Bold('Average (msec)')),
                                      TableHeader(Bold('Max (msec)')),
                                      TableHeader(Bold('Std.Dev. (msec)'))]))
        for vc_type in sorted(self.view_controllers.keys()):
            stats = self.view_controllers[vc_type].samples['IN_USE'].statistics
            self._report_one_inuse_row(table_usage, vc_type, stats)

        paragraphs = [Paragraph([Bold('UI View Controller Usage'),
                                 table_usage]),
                      Paragraph([Bold('UI View Controller Transition'),
                                 table_transition])]
        return paragraphs

    def attachment(self):
        return None