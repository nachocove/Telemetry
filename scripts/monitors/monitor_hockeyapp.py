import os
from dateutil.parser import parse
import zipfile
import logging
from AWS.events import Event
import HockeyApp
from boto.dynamodb2.layer1 import DynamoDBConnection
from AWS.query import Query
from AWS.selectors import SelectorEqual, SelectorStartsWith, SelectorLessThan
from monitor_base import Monitor
from misc.utc_datetime import UtcDateTime
from logtrace import LogTrace
from misc.html_elements import *


class CrashInfo:
    device_info_logs = None

    """
    A class that ties a HockeyApp Crash object with its associated raw log and telemetry event trace.
    """
    def __init__(self, ha_crash_obj, conn):
        assert isinstance(ha_crash_obj, HockeyApp.crash.Crash)
        assert isinstance(conn, DynamoDBConnection)

        self.logger = logging.getLogger('monitor')
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
        Determine the client id from the build info
        """
        # Get the description and parse the parameters
        description = self.ha_crash_obj.read_description()
        if description is None:
            return None
        ha_desc_obj = HockeyApp.crash.CrashDescription(description)
        if ha_desc_obj.device_id is None:
            self.logger.warning('    no device ID to correlate with telemetry logs')
            return None
        else:
            self.logger.debug('    device id = %s', ha_desc_obj.device_id)

        query = Query()
        query.add('device_id', SelectorEqual(ha_desc_obj.device_id))
        # TODO Should probably see if we can also have a lower bound here, i.e. the upper bound is the crash timestamp.
        query.add('uploaded_at', SelectorLessThan(Event.parse_datetime(UtcDateTime(parse(self.crash_utc)))))
        # query telemetry.device_info for the client. Look for the latest entry no greater than the crash date.
        devices = Query.users(query, self.conn)
        if devices:
            last_device = sorted(devices, key=lambda device: device['uploaded_at'])[-1]
            client = last_device['client']
            self.logger.debug('     Found client %s in telemetry.device_info', client)
        else:
            self.logger.debug('     No client found in telemetry.device_info. Searching in telemetry.log (this could take a while)')
            # Query telemetry.log for the build info
            if CrashInfo.device_info_logs is None:
                query = Query()
                query.add('event_type', SelectorEqual('INFO'))
                query.add('message', SelectorStartsWith('Device ID: '))
                CrashInfo.device_info_logs = Query.events(query, self.conn)
            events = list()
            for event in CrashInfo.device_info_logs:
                if event['message'].startswith('Device ID: ' + ha_desc_obj.device_id):
                    events.append(event)
            if len(events) == 0:
                self.logger.warning('    cannot find build info log for crash %s', self.ha_crash_obj.crash_id)
                return None
            assert 'client' in events[0]
            client = events[0]['client']
        self.logger.debug('    client = %s', client)
        return client

    def _get_trace(self):
        trace = None
        self.crash_utc = self._determine_crash_time()
        if self.crash_utc is None:
            self.logger.warning('    cannot find crash time')
            self.events = None
            return trace
        else:
            self.logger.debug('    crash time = %s', self.crash_utc)
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
        if self.crash_utc is None:
            return None
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

class CrashInfoUnknownPlatformException(Exception):
    pass

def CrashInfoFactory(ha_crash_obj, conn):
    """
    Create a crashinfo subclass based on the crash-group

    :param ha_crash_obj: a crash object
    :type ha_crash_obj: HockeyApp.crash.Crash()
    :param conn: an aws connection
    :type conn: AWS()
    :return:
    """
    assert(isinstance(ha_crash_obj, HockeyApp.crash.Crash))
    platform = ha_crash_obj.crash_group_obj.app_obj.platform
    if platform == 'iOS':
        return CrashInfoIos(ha_crash_obj, conn)
    else:
        raise CrashInfoUnknownPlatformException("Unsupported (unimplemented) crash platform %s" % platform)

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
        self.logger.info('Query crash logs...')
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
                self.logger.debug('  Analyzing crash %s (in crash group %s, platform %s)',
                                  crash.crash_id, crash_group.crash_group_id, crash_group.app_obj.platform)
                # HockeyApp is quite slow. By the time we get here,
                # the DynamoDB connection may have time out. So, create
                # a new one
                conn = self.clone_connection(self.conn)
                try:
                    self.crashes.append(CrashInfoFactory(crash, conn))
                except CrashInfoUnknownPlatformException as e:
                    self.logger.error('Could not analyze crash: %s (SKIPPING)' % e)

    def report(self, summary, **kwargs):
        summary.add_entry('Crash count', str(len(self.crashes)))

        if len(self.crashes) == 0:
            return None

        table = Table()
        row = TableRow([TableHeader(Bold('Time')),
                        TableHeader(Bold('Client')),
                        TableHeader(Bold('Reason'))])
        table.add_row(row)
        for crash in self.crashes:
            crash_obj = crash.ha_crash_obj
            # Limit reason to 5 lines
            if crash_obj.crash_group_obj.reason is not None:
                reason = crash_obj.crash_group_obj.reason.encode('utf-8')
            else:
                reason = '<unknown>'
            lines = reason.split('\n')
            max_lines = 3
            if len(lines) > max_lines:
                lines = lines[:max_lines]
            reason = '\n'.join(lines)
            client = '<unknown>'
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
        if len(self.crashes) == 0:
            return None
        raw_log_prefix = '%s_%s' % (self.desc, self.end.file_suffix())
        zipped_file_path = raw_log_prefix + '.zip'
        zipped_file = zipfile.ZipFile(zipped_file_path, 'w', zipfile.ZIP_DEFLATED)

        for crash in self.crashes:
            crash_log_path = crash.save_log()
            if crash_log_path is None:
                continue
            zipped_file.write(crash_log_path)
            os.unlink(crash_log_path)
            crash_trace_path = crash.save_trace()
            if crash_trace_path is not None:
                # TODO - for some reason, all traces are empty. Leave it out for now
                #zipped_file.write(crash_trace_path)
                os.unlink(crash_trace_path)

        zipped_file.close()
        return zipped_file_path
