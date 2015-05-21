# Copyright 2014, NachoCove, Inc
from datetime import timedelta, datetime
import re
from AWS.query import Query
from AWS.s3_telemetry import get_s3_events
from AWS.selectors import SelectorEqual, SelectorContains
from misc.html_elements import Table, TableRow, TableHeader, Bold, TableElement, Text, Paragraph, Link, ListItem, \
    UnorderedList
from misc.number_formatter import pretty_number
from misc.support import Support, SupportBackLogEvent, SupportSha256EmailAddressEvent
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
            pinger_telemetry[key] = sorted([ev for ev in all_events if self.start <= ev['timestamp'] < self.end], key=lambda x: x['timestamp'])
        else:
            self.logger.info('Pulling results from cache')
        return pinger_telemetry[key]

    def report(self, summary, **kwargs):
        summary.add_entry(self.desc, pretty_number(len(self.events)))
        if self.events:
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
        else:
            return None


    def attachment(self):
        return None

class MonitorPingerPushMessages(MonitorPinger):
    def __init__(self, look_ahead=None, *args, **kwargs):
        kwargs.setdefault('desc', 'Missed Push messages')
        MonitorPinger.__init__(self, *args, **kwargs)
        self.unprocessed_push = []
        self.fetch_before_push = []
        self.no_telemetry_found = []
        self.push_received = []
        self.push_superceded = []
        self.push_missed_by_client = {}
        self.look_ahead = look_ahead if look_ahead is not None else 180

    def run(self):
        context_re = re.compile(r'context (?P<context>[a-z0-9]{8})')
        self.events = [ev for ev in super(MonitorPingerPushMessages, self).run() if "Sending push message" in ev['message']]
        for ev in self.events:
            if 'context' not in ev:
                m = context_re.search(ev['message'])
                ev['context'] = m.group('context') if m else ''
            ev.setdefault("session", "")
            ev.setdefault("device", "")

        self.logger.debug("Found %d push events", len(self.events))
        time_frame = timedelta(minutes=self.look_ahead)
        for push in self.events:
            push['annotations'] = []
            push_start = push['timestamp']
            push_end = UtcDateTime(push['timestamp'].datetime + time_frame)
            push_received_event = None
            newer_push_received_event = None
            query = Query()
            query.add('event_type', SelectorEqual('INFO'))
            query.add('client', SelectorEqual(push['client']))
            # use timestamp here, instead of uploaded_at, since we're interested in the events
            # that HAPPENED after the push, and not those uploaded around the push (those could be older).
            query.add_range('timestamp', push_start, push_end)
            events, count = self.query_all(query)
            if not events:
                query = Query()
                query.add('event_type', SelectorEqual('SUPPORT'))
                query.add('client', SelectorEqual(push['client']))
                # again use timestamp here to get records that happened around the time of the push, not those
                # merely uploaded then (which could be older)
                query.add_range('timestamp', push_start, push_end)
                support_events, _ = self.query_all(query)
                if not support_events:
                    # Look for ANY telemetry.
                    query = Query()
                    query.add('client', SelectorEqual(push['client']))
                    query.add_range('uploaded_at', push_start, push_end)
                    nx, _ = self.query_all(query)
                    if not nx:
                        query = Query()
                        query.add('client', SelectorEqual(push['client']))
                        query.add_range('timestamp', push_start, push_end)
                        nx, _ = self.query_all(query)
                        if not nx:
                            push['annotations'].append("No client telemetry (of any kind) uploaded in timeframe %s - %s.", push_start, push_end)
                        else:
                            push['annotations'].append("Some activity, but no usable client telemetry found in timeframe (%s)", nx)

                    else:
                        push['annotations'].append("No client telemetry (INFO, SUPPORT) found.")
                else:
                    backlogged_events = sorted(Support.filter(support_events, [SupportBackLogEvent]), reverse=True, key=lambda x: x.timestamp)
                    sha_256_events = sorted(Support.filter(support_events, [SupportSha256EmailAddressEvent]), reverse=True, key=lambda x: x.timestamp)
                    if backlogged_events:
                        push['annotations'].append("Client is backlogged: num_events %s oldest_event %s" % (backlogged_events[0].num_events, backlogged_events[0].oldest_event))
                    elif sha_256_events:
                        push['annotations'].append("Only Support heartbeats (latest %s)" % sha_256_events[0].timestamp)
                    else:
                        push['annotations'].append("Only Support request (%s)" % [ x for x in support_events])

                self.no_telemetry_found.append(push)
                continue

            perform_fetch = None
            for ev in events:
                if 'PerformFetch called' in ev['message']:
                    if not push_received_event:
                        perform_fetch = perform_fetch or ev
                elif 'Got remote notification' in ev['message']:
                    m = re.search("ses[sion]? = (?P<session>[a-f0-9]+);(.*)time = (?P<time>[0-9]+);", ev['message'].replace('\n', ''))
                    if m:
                        t = UtcDateTime(datetime.utcfromtimestamp(int(m.group('time'))))
                        if m.group('session') == push['session']:
                            push_received_event = ev
                            break
                        elif t > push['timestamp']:
                            newer_push_received_event = ev
                            push['annotations'].append("Newer push superceded this one.")
                            break
                        else:
                            self.logger.warn("Push seen that is not for this session, and not newer than this push! %s, %s", push, ev['message'])
                    else:
                        self.logger.warn("Could not process 'Got remote notification' message %s", ev['message'])

            if not push_received_event and not newer_push_received_event:
                if push['client'] not in self.push_missed_by_client:
                    self.push_missed_by_client[push['client']] = {}
                if push['device'] not in self.push_missed_by_client[push['client']]:
                    self.push_missed_by_client[push['client']][push['device']] = []
                self.push_missed_by_client[push['client']][push['device']].append(push)
                push['annotations'].append("No Push received in %s" % time_frame)
                self.unprocessed_push.append(push)
            elif newer_push_received_event:
                self.push_superceded.append(push)
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
        avg = sum/len(events) if len(events) != 0 else 0
        return min if min < 10000000 else 0, avg, max

    def report(self, summary, **kwargs):
        paragraph_elements = []
        summary.add_entry("Pushes sent", pretty_number(len(self.events)))
        if len(self.push_received) > 0:
            summary.add_entry("Pushes processed", pretty_number(len(self.push_received)))
        if len(self.push_superceded) > 0:
            summary.add_entry("Pushes superceded", pretty_number(len(self.push_superceded)))
        if len(self.no_telemetry_found) > 0:
            summary.add_entry("Pushes not analyzed (no client telemetry found)", pretty_number(len(self.no_telemetry_found)))
        if len(self.events) - len(self.no_telemetry_found) > 0:
            summary.add_entry("Look-Ahead", pretty_number(self.look_ahead))
            summary.add_entry("Push received (min/avg/max)", "%.2f/%.2f/%.2f" % self.min_avg_max(self.push_received))
            summary.add_entry("# Push missed", pretty_number(len(self.unprocessed_push)))
            summary.add_entry("% Push missed", pretty_number(percentage(len(self.events), len(self.unprocessed_push))))
            summary.add_entry("Devices with problems", pretty_number(len(self.push_missed_by_client)))

        # if self.unprocessed_push:
        #     paragraph_elements.append(Bold(self.title()))
        #     paragraph_elements.append(self.table_from_events(self.unprocessed_push))
        #
        if self.push_missed_by_client:
            paragraph_elements.append(Bold(self.title()))
            table = Table()
            table.add_row(TableRow([TableHeader(Bold('Time Push Sent(UTC)')),
                                    TableHeader(Bold('Client')),
                                    TableHeader(Bold('Sessions')),
                                    ]))
            for client in self.push_missed_by_client:
                telemetry_items = list()
                for device in self.push_missed_by_client[client]:
                    for ev in self.push_missed_by_client[client][device]:
                        if ev['client']:
                            link = get_client_telemetry_link(self.prefix, client, ev['timestamp'])
                        else:
                            link = get_pinger_telemetry_link(self.prefix, ev['timestamp'])

                        telemetry_items.append(ListItem([Text(device),
                                                         UnorderedList([ListItem(Text("Session %s" % ev['session'])),
                                                                        ListItem(Text("%s:%s" % (ev['device'], ev['context']))),
                                                                        ListItem(Text("\n".join(ev['annotations']))),
                                                                        ListItem(Link('%s Telemetry - %s' % (self.prefix.capitalize(), ev['timestamp']), link)),
                                                                        ])]))

                row = TableRow([TableElement(Text(str(ev['timestamp']))),
                                TableElement(Text(ev['client'])),
                                TableElement(UnorderedList(telemetry_items)),
                                ])
                table.add_row(row)
            paragraph_elements.append(table)
        if self.fetch_before_push:
            paragraph_elements.append(Bold("Pushes received after PerformFetch"))
            paragraph_elements.append(self.table_from_events(self.fetch_before_push))
        if self.no_telemetry_found:
            paragraph_elements.append(Bold("Pushes sent, but no telemetry found (yet)"))
            paragraph_elements.append(self.table_from_events(self.no_telemetry_found))

        return Paragraph(paragraph_elements)

    def attachment(self):
        return None

def percentage(whole, part):
    return 100 * float(part)/float(whole) if whole != 0 else 0

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
