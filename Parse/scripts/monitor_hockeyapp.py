import re
import zipfile
import HockeyApp
import Parse
from monitor_base import Monitor
from Parse.utc_datetime import UtcDateTime
from logtrace import LogTrace
from html_elements import *
from datetime import timedelta


class CrashInfo:
    """
    A class that ties a HockeyApp Crash object with its associated raw log and telemetry event trace.
    """
    def __init__(self, ha_crash_obj, conn):
        assert isinstance(ha_crash_obj, HockeyApp.crash.Crash)
        assert isinstance(conn, Parse.connection.Connection)

        self.ha_crash_obj = ha_crash_obj
        self.conn = conn
        self.log = self.ha_crash_obj.read_log()
        self.crash_utc = self._determine_crash_time()  # require initialized self.log
        self.trace = self._get_trace()

    def _determine_crash_time(self):
        """
        Each actual (derived) class must implement this method. In general,
        the method to extract the crash time from a crash log is not the same
        in each platform.
        """
        raise NotImplementedError("subclass must implement this method")

    def _determine_client(self):
        """
        Determine the Parse client id from the build info
        """
        # Get the description and parse the parameters
        description = self.ha_crash_obj.read_description()
        if description is None:
            return None
        lines = description.split('\n')

        def parse_line(name, line):
            match = re.search('^' + name + ': (?P<value>\S+)', line)
            assert match
            return match.group('value')

        version = parse_line('Version', lines[0])
        build_number = parse_line('Build Number', lines[1])
        launch_time = parse_line('Launch Time', lines[2])

        # Query telemetry for the build
        query = Parse.query.Query()
        query.add('event_type', Parse.query.SelectorEqual('INFO'))
        build_info_str = re.escape('%s (build %s)' % (version, build_number))
        query.add('message', Parse.query.SelectorContain(build_info_str))
        start = UtcDateTime(launch_time)
        stop = UtcDateTime(launch_time)
        start.datetime -= timedelta(seconds=5)
        stop.datetime += timedelta(seconds=5)
        query.add('timestamp', Parse.query.SelectorGreaterThanEqual(start))
        query.add('timestamp', Parse.query.SelectorLessThanEqual(stop))
        events = Parse.query.Query.objects('Events', query, self.conn)[0]
        if len(events) == 0:
            print 'WARN: cannot find build info log for crash %s' % self.ha_crash_obj.crash_id
            return None
        if len(events) > 1:
            print 'WARN: more than one matching events... returning 1st one (crash %s)' % self.ha_crash_obj.crash_id
        assert 'client' in events[0]
        return events[0]['client']

    def _get_trace(self):
        trace = None
        if self.crash_utc is None:
            self.events = None
            return trace
        client = self._determine_client()
        if client is None:
            return trace
        (start, end) = LogTrace.get_time_window(self.crash_utc, 2, 0)
        desc = 'crash_trace.%s' % self.ha_crash_obj.crash_id
        trace = LogTrace(desc, client, start, end)
        trace.query(self.conn)
        return trace

    def save_trace(self):
        if self.trace is None:
            return
        return self.trace.write_file()

    def save_log(self):
        fname = 'crash_log.%s.%s.txt' % (self.ha_crash_obj.crash_id,
                                         UtcDateTime(self.crash_utc).file_suffix())
        with open(fname, 'w') as f:
            f.write(self.log)
            f.close()
        return fname


class CrashInfoIos(CrashInfo):
    def __init__(self, ha_crash_obj, conn):
        CrashInfo.__init__(self, ha_crash_obj, conn)

    def _determine_crash_time(self):
        for line in self.log.split('\n')[:11]:
            match = re.search('^Date/Time:(\s+)(?P<crash_utc>\S+)$', line)
            if not match:
                continue
            return match.group('crash_utc')
        return None  # somehow cannot find the crash time in the raw crash log


class MonitorHockeyApp(Monitor):
    def __init__(self, conn, start=None, end=None, ha_app_obj=None):
        Monitor.__init__(self, conn, "crashes", start, end)
        assert isinstance(ha_app_obj, HockeyApp.app.App)
        self.ha_app_obj = ha_app_obj
        self.crashes = list()

    def _within_window(self, datetime):
        assert isinstance(datetime, UtcDateTime)
        if self.start is not None:
            assert isinstance(self.start, UtcDateTime)
            if datetime < self.start:
                return False
        if self.end is not None:
            assert isinstance(self.end, UtcDateTime)
            if datetime >= self.end:
                return False
        return True

    def run(self):
        self.crashes = list()
        for crash_group in self.ha_app_obj.crash_groups():
            # Look for all crash groups that have been updated within the time window
            if not self._within_window(UtcDateTime(crash_group.last_crash_at)):
                continue  # no recent update
            crash_list = crash_group.crashes()
            for crash in crash_list:
                # Get all crashes that uploaded within the time window
                if not self._within_window(UtcDateTime(crash.created_at)):
                    continue
                # TODO - parse the platform and instantiate the right class of objects
                self.crashes.append(CrashInfoIos(crash, self.conn))

    def report(self, summary):
        summary.add_entry('Crash count', str(len(self.crashes)))
        table = Table()
        row = TableRow([TableHeader(Bold('Time')),
                        TableHeader(Bold('Reason'))])
        table.add_row(row)
        for crash in self.crashes:
            crash_obj = crash.ha_crash_obj
            row = TableRow([TableElement(Text(crash_obj.created_at)),
                            TableElement(Text(crash_obj.crash_group_obj.reason.encode('utf-8')))])
            table.add_row(row)

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

    def attachment(self):
        raw_log_prefix = '%s_%s' % (self.desc, self.end.file_suffix())
        zipped_file_path = raw_log_prefix + '.zip'
        zipped_file = zipfile.ZipFile(zipped_file_path, 'w', zipfile.ZIP_DEFLATED)

        for crash in self.crashes:
            fname = crash.save_log()
            zipped_file.write(fname)
            fname = crash.save_trace()
            zipped_file.write(fname)

        zipped_file.close()
        return zipped_file_path
