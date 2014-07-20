import os
import zipfile
import HockeyApp
import Parse
from monitor_base import Monitor
from Parse.utc_datetime import UtcDateTime
from logtrace import LogTrace
from html_elements import *


class CrashInfo:
    """
    A class that ties a HockeyApp Crash object with its associated raw log and telemetry event trace.
    """
    def __init__(self, ha_crash_obj, conn):
        assert isinstance(ha_crash_obj, HockeyApp.crash.Crash)
        assert isinstance(conn, Parse.connection.Connection)

        self.ha_crash_obj = ha_crash_obj
        self.ha_desc_obj = None
        self.conn = conn
        self.log = self.ha_crash_obj.read_log()
        self.crash_utc = self._determine_crash_time()  # require initialized self.log
        self.client = self._determine_client()
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
        ha_desc_obj = HockeyApp.crash.CrashDescription(description)
        if ha_desc_obj.device_id is None:
            print '    WARN: no device ID to correlate with telemetry logs'
            return None
        else:
            print '    device id = %s' % ha_desc_obj.device_id

        # Query telemetry for the build
        query = Parse.query.Query()
        query.add('event_type', Parse.query.SelectorEqual('INFO'))
        query.add('message', Parse.query.SelectorStartsWith('Device ID: ' + ha_desc_obj.device_id))
        events = Parse.query.Query.objects('Events', query, self.conn)[0]
        if len(events) == 0:
            print '    WARN: cannot find build info log for crash %s' % self.ha_crash_obj.crash_id
            return None
        assert 'client' in events[0]
        client = events[0]['client']
        print '    client = %s' % client
        return client

    def _get_trace(self):
        trace = None
        self.crash_utc = self._determine_crash_time()
        if self.crash_utc is None:
            print '    WARN: cannot find crash time'
            self.events = None
            return trace
        else:
            print '    crash time = %s' % self.crash_utc
        if self.client is None:
            return trace
        (start, end) = LogTrace.get_time_window(self.crash_utc, 2, 0)
        desc = 'crash_trace.%s' % self.ha_crash_obj.crash_id
        trace = LogTrace(desc, self.client, start, end)
        trace.query(self.conn)
        return trace

    def save_trace(self):
        if self.trace is None:
            return None
        return self.trace.write_file()

    def save_log(self):
        log_path = 'crash_log.%s.%s.txt' % (self.ha_crash_obj.crash_id,
                                            UtcDateTime(self.crash_utc).file_suffix())
        with open(log_path, 'w') as f:
            f.write(self.log)
            f.close()
        return log_path


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
        print 'Query crash logs...'
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
                print '  Analyzing crash %s (in crash group %s)' % (crash.crash_id, crash_group.crash_group_id)
                # HockeyApp is quite slow. By the time we get here,
                # the Parse connection would have time out. So, create
                # a new one
                conn = self.clone_connection(self.conn)
                # TODO - parse the platform and instantiate the right class of objects
                self.crashes.append(CrashInfoIos(crash, conn))

    def report(self, summary):
        summary.add_entry('Crash count', str(len(self.crashes)))
        table = Table()
        row = TableRow([TableHeader(Bold('Time')),
                        TableHeader(Bold('Client')),
                        TableHeader(Bold('Reason'))])
        table.add_row(row)
        for crash in self.crashes:
            crash_obj = crash.ha_crash_obj
            # Limit reason to 5 lines
            reason = crash_obj.crash_group_obj.reason.encode('utf-8')
            lines = reason.split('\n')
            max_lines = 3
            if len(lines) > max_lines:
                lines = lines[:max_lines]
            reason = '\n'.join(lines)
            client = '-'
            if crash.client:
                client = crash.client
            link = 'https://rink.hockeyapp.net/manage/apps/%s/app_versions/1/crash_reasons/%s?type=crashes' % \
                   (self.ha_app_obj.id, crash_obj.crash_group_obj.crash_group_id)
            row = TableRow([TableElement(Text(crash_obj.created_at)),
                            TableElement(Text(client)),
                            TableElement(Link(Text(reason, keep_linefeed=True), link))])
            table.add_row(row)

        title = self.title()
        paragraph = Paragraph([Bold(title), table])
        return paragraph

    def attachment(self):
        raw_log_prefix = '%s_%s' % (self.desc, self.end.file_suffix())
        zipped_file_path = raw_log_prefix + '.zip'
        zipped_file = zipfile.ZipFile(zipped_file_path, 'w', zipfile.ZIP_DEFLATED)

        for crash in self.crashes:
            crash_log_path = crash.save_log()
            zipped_file.write(crash_log_path)
            os.unlink(crash_log_path)
            crash_trace_path = crash.save_trace()
            if crash_trace_path is not None:
                # TODO - for some reason, all traces are empty. Leave it out for now
                #zipped_file.write(crash_trace_path)
                os.unlink(crash_trace_path)

        zipped_file.close()
        return zipped_file_path
