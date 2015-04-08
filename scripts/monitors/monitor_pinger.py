# Copyright 2014, NachoCove, Inc
from datetime import timedelta
from AWS.query import Query
from AWS.s3_telemetry import get_s3_events
from AWS.selectors import SelectorEqual, SelectorContains
from misc.html_elements import Table, TableRow, TableHeader, Bold, TableElement, Text, Paragraph, Link
from misc.number_formatter import pretty_number
from misc.utc_datetime import UtcDateTime
from monitors.monitor_base import Monitor, get_client_telemetry_link, get_pinger_telemetry_link


pinger_telemetry = {}
class MonitorPinger(Monitor):
    def __init__(self, s3conn=None, bucket_name=None, path_prefix=None, *args, **kwargs):
        Monitor.__init__(self, *args, **kwargs)
        self.s3conn = s3conn
        self.bucket_name = bucket_name
        self.path_prefix = path_prefix
        self.events = []

    def run(self):
        global pinger_telemetry
        key = "%s--%s" % (str(self.start), str(self.end))
        if not key in pinger_telemetry:
            self.logger.info('Querying %s...', self.desc)
            all_events = get_s3_events(self.s3conn, self.bucket_name, self.path_prefix, "log", self.start, self.end, logger=self.logger)
            pinger_telemetry[key] = sorted([ev for ev in all_events if self.start <= ev['timestamp'] < self.end], key=lambda x: ev['timestamp'])
        else:
            self.logger.info('Pulling results from cache')
        return pinger_telemetry[key]

    def report(self, summary, **kwargs):
        summary.add_entry(self.desc, pretty_number(len(self.events)))
        table = Table()
        table.add_row(TableRow([TableHeader(Bold('Time (UTC)')),
                                TableHeader(Bold('Message')),
                                TableHeader(Bold('%s telemetry' % self.prefix.capitalize())),
                                ]))
        for ev in self.events:
            if ev['client']:
                link = get_client_telemetry_link(self.prefix, ev['client'], ev['timestamp'])
            else:
                link = get_pinger_telemetry_link(self.prefix, ev['timestamp'])
            row = TableRow([TableElement(Text(str(ev['timestamp']))),
                            TableElement(Text(ev['message'])),
                            TableElement(Link('Telemetry', link)),
                            ])
            table.add_row(row)
        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph


    def attachment(self):
        return None

class MonitorPingerPushMessages(MonitorPinger):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'Missed Push messages')
        MonitorPinger.__init__(self, *args, **kwargs)
        self.unprocessed_push = []
        self.fetch_before_push = []
        self.push_received = []

    def run(self):
        all_events = super(MonitorPingerPushMessages, self).run()
        self.events = [ev for ev in all_events if "Sending push message" in ev['message']]
        self.logger.debug("Found %d push events", len(self.events))
        time_frame = timedelta(minutes=30)
        for push in self.events:
            push['annotations'] = []
            push_received_event = None
            query = Query()
            query.add('event_type', SelectorEqual('INFO'))
            query.add('client', SelectorEqual(push['client']))
            query.add_range('timestamp', push['timestamp'], UtcDateTime(push['timestamp'].datetime + time_frame))
            events, count = self.query_all(query)
            perform_fetch = None
            for ev in events:
                if 'PerformFetch called' in ev['message']:
                    if not push_received_event:
                        perform_fetch = perform_fetch or ev
                elif 'Got remote notification' in ev['message'] and "session = %s" % push['session'] in ev['message']:
                    push_received_event = ev
                    break

            if not push_received_event:
                if not events:
                    push['annotations'].append("No client events found. Perhaps no telemetry upload yet?")
                else:
                    push['annotations'].append("No Push received in %s" % time_frame)

                self.unprocessed_push.append(push)
            else:
                self.push_received.append((push, push_received_event))

            if push_received_event and perform_fetch:
                self.fetch_before_push.append(push)

    def table_from_events(self, events):
        table = Table()
        table.add_row(TableRow([TableHeader(Bold('Time Push Sent(UTC)')),
                                TableHeader(Bold('Client')),
                                TableHeader(Bold('Session')),
                                TableHeader(Bold('%s telemetry' % self.prefix.capitalize())),
                                TableHeader(Bold('Annotations')),
                                ]))
        for ev in events:
            if ev['client']:
                link = get_client_telemetry_link(self.prefix, ev['client'], ev['timestamp'])
            else:
                link = get_pinger_telemetry_link(self.prefix, ev['timestamp'])
            row = TableRow([TableElement(Text(str(ev['timestamp']))),
                            TableElement(Text(ev['client'])),
                            TableElement(Text(ev['session'])),
                            TableElement(Link('Telemetry', link)),
                            TableElement(Text("\n".join(ev['annotations']))),
                            ])
            table.add_row(row)
        return table

    def min_avg_max(self, events):
        min = 10000000
        max = 0
        sum = 0
        for push,recv in events:
            delta = recv['timestamp'] - push['timestamp']
            sum += delta
            if delta < min:
                min = delta
            if delta > max:
                max = delta
        return min, sum/len(events), max

    def report(self, summary, **kwargs):
        summary.add_entry("Pushes sent", pretty_number(len(self.events)))
        summary.add_entry("Push received (min/avg/max)", "%.2f/%.2f/%.2f" % self.min_avg_max(self.push_received))
        summary.add_entry("Push missed", pretty_number(len(self.unprocessed_push)))
        summary.add_entry("Percent Push missed", pretty_number(percentage(len(self.events), len(self.unprocessed_push))))
        paragraph_elements = []
        if self.unprocessed_push:
            paragraph_elements.append(Bold(self.title()))
            paragraph_elements.append(self.table_from_events(self.unprocessed_push))

        if self.fetch_before_push:
            paragraph_elements.append(Bold("Pushes received after PerformFetch"))
            paragraph_elements.append(self.table_from_events(self.fetch_before_push))

        return Paragraph(paragraph_elements)

    def attachment(self):
        return None

def percentage(whole, part):
    return 100 * float(part)/float(whole) if whole != 0 else 100

class MonitorPingerErrors(MonitorPinger):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'Pinger errors')
        MonitorPinger.__init__(self, *args, **kwargs)

    def run(self):
        all_events = super(MonitorPingerErrors, self).run()
        self.events = [ev for ev in all_events if ev['event_type'] == 'ERROR']

class MonitorPingerWarnings(MonitorPinger):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'Pinger warnings')
        MonitorPinger.__init__(self, *args, **kwargs)

    def run(self):
        all_events = super(MonitorPingerWarnings, self).run()
        self.events = [ev for ev in all_events if ev['event_type'] == 'WARNING']

class MonitorClientPingerIssues(MonitorPinger):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('desc', 'Client-side Pinger errors/warnings')
        MonitorPinger.__init__(self, *args, **kwargs)
        self.events = []
        self.debug = False

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        event_types = ['WARN', 'ERROR']
        if self.debug:
            event_types.append('INFO')
        for event_type in event_types:
            query = Query()
            query.add('event_type', SelectorEqual(event_type))
            query.add('message', SelectorContains('[PA]'))
            query.add_range('uploaded_at', self.start, self.end)
            self.events.extend(Query.events(query, self.conn))

    def report(self, summary, **kwargs):
        summary.add_entry("Client warnings", pretty_number(len([ev for ev in self.events if ev['event_type'] == 'WARN'])))
        summary.add_entry("Client errors", pretty_number(len([ev for ev in self.events if ev['event_type'] == 'ERROR'])))
        if self.events:
            table = Table()
            table.add_row(TableRow([TableHeader(Bold('Time Push Sent(UTC)')),
                                    TableHeader(Bold('Event')),
                                    TableHeader(Bold('Client')),
                                    TableHeader(Bold('Message')),
                                    TableHeader(Bold('%s telemetry' % self.prefix.capitalize())),
                                    ]))
            for ev in self.events:
                if ev['client']:
                    link = get_client_telemetry_link(self.prefix, ev['client'], ev['timestamp'])
                else:
                    link = get_pinger_telemetry_link(self.prefix, ev['timestamp'])
                row = TableRow([TableElement(Text(str(ev['timestamp']))),
                                TableElement(Text(ev['event_type'])),
                                TableElement(Text(ev['client'])),
                                TableElement(Text(ev['message'])),
                                TableElement(Link('Telemetry', link)),
                                ])
                table.add_row(row)
            return Paragraph([Bold(self.title()), table])
        else:
            return None
