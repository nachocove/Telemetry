# Copyright 2014, NachoCove, Inc
import json
import re
import zlib
from boto.s3.connection import S3Connection
from misc.utc_datetime import UtcDateTime
from datetime import datetime
import hashlib

T3_EVENT_CLASS_FILE_PREFIXES = {
         'ALL': ['PROTOCOL','LOG', 'COUNTER', 'STATISTICS2','UI', 'SUPPORT', 'DEVICEINFO','PINGER', 'SAMPLES', 'TIMESERIES'],
         'PROTOCOL': 'protocol',
         'LOG': 'log',
         'COUNTER': 'counter',
         'STATISTICS2': 'statistics2',
         'UI': 'ui',
         'SUPPORT': 'support',
         'DISTRIBUTION': 'distribution',
         'SAMPLES': 'samples',
         'TIMESERIES': 'time_series',
         'DEVICEINFO': 'device_info',
         'PINGER': 'plog',
         'TROUBLETICKETS': 'trouble_tickets',
}

def get_pinger_events(conn, bucket_name, userid, deviceid, after, before, search, logger=None):
    logger.info("Getting events of class PINGER for userid %s deviceid %s from %s to %s with search '%s'" %
                (userid, deviceid, after, before, search))
    assert isinstance(conn, S3Connection)
    bucket = conn.get_bucket(bucket_name)
    #sample key
    #c6ae00d0-e259-4bfc-903d-5b6bc62cd651-dev-pinger
    #20150529/plog-20150529063152823.gz
    events = []
    prev_file_uploaded_at_ts = None
    for date_prefix in get_T3_date_prefixes(after, before):
        get_prefix = date_prefix
        logger.debug("get_prefix is %s" % get_prefix)
        file_regex = re.compile(r'.*/%s-(?P<uploaded_at>[0-9]+).gz' % T3_EVENT_CLASS_FILE_PREFIXES['PINGER'])

        search_regex=re.compile(search)
        for key in bucket.list(prefix=get_prefix):
            m = file_regex.match(key.key)
            if m is not None:
                uploaded_at = m.group('uploaded_at')
                uploaded_at_ts = datetime.strptime(uploaded_at, '%Y%m%d%H%M%S%f')
                uploaded_at_ts = UtcDateTime(uploaded_at_ts)
                if is_pinger_file_in_date_range(logger, uploaded_at_ts, before, after, prev_file_uploaded_at_ts):
                    file_content = zlib.decompress(key.get_contents_as_string(), 16+zlib.MAX_WBITS)
                    for line in file_content.splitlines():
                        ev = json.loads(line)
                        timestamp = UtcDateTime(ev['timestamp'])
                        if not (timestamp.datetime >= after.datetime and timestamp.datetime < before.datetime):
                            continue
                        if search != '':
                            sm = search_regex.search(line)
                        else:
                            sm = None
                        if search == '' or sm is not None:
                            if userid and (('client' in ev and ev['client'] != userid) or ('client' not in ev)):
                                continue
                            if deviceid and (('device' in ev and ev['device'] != deviceid) or ('device' not in ev)):
                                continue
                            if 'device' in ev:
                                ev['device_id'] = ev['device']
                                del ev['device']
                            else:
                                ev['device_id'] = ""
                            if 'client' in ev:
                                ev['user_id'] = ev['client']
                                del ev['client']
                            else:
                                ev['user_id'] = ""
                            ev['timestamp'] = timestamp
                            ev['uploaded_at'] = uploaded_at_ts
                            events.append(ev)
                    prev_file_uploaded_at_ts = uploaded_at_ts
    if logger:
        logger.debug("Found %d PINGER events.", len(events))
    return events

def get_client_events(conn, bucket_name, userid, deviceid, after, before, event_class, search, threadid=0, logger=None, event_type=None):
    logger.info("Getting events of class %s for userid %s deviceid %s from %s to %s with search='%s' for threadid=%d for event_type %s" %
                (event_class, userid, deviceid, after, before, search, threadid, event_type))
    assert isinstance(conn, S3Connection)
    bucket = conn.get_bucket(bucket_name)
    if userid:
        client_prefix = '/' + hashlib.sha256(userid).hexdigest()[0:8] + '/' + userid
        if deviceid:
            client_prefix += '/' + deviceid + '/NachoMail/' + T3_EVENT_CLASS_FILE_PREFIXES[event_class]
    else:
        client_prefix = ''
    events = []
    prev_key = None
    prev_key_uploaded_at_ts = None
    first_in_date_range_file_processed = False
    for date_prefix in get_T3_date_prefixes(after, before):
        get_prefix = date_prefix + client_prefix
        logger.debug("get_prefix is %s" % get_prefix)
        userid_regex = '\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+'
        if not userid:
            if deviceid:
                file_regex = re.compile(r'.*/(?P<user_id>%s)/%s/NachoMail/%s-(?P<created_at>[0-9]+).gz' % (userid_regex, deviceid, T3_EVENT_CLASS_FILE_PREFIXES[event_class]))
            else:
                file_regex = re.compile(r'.*/(?P<user_id>%s)/(?P<device_id>Ncho\w+)/NachoMail/%s-(?P<created_at>[0-9]+).gz' % (userid_regex, T3_EVENT_CLASS_FILE_PREFIXES[event_class]))
        else:
            if not deviceid:
                file_regex = re.compile(r'.*/(?P<device_id>Ncho\w+)/NachoMail/%s-(?P<created_at>[0-9]+).gz' % T3_EVENT_CLASS_FILE_PREFIXES[event_class])
            else:
                file_regex = re.compile(r'.*%s-(?P<created_at>[0-9]+).gz' % T3_EVENT_CLASS_FILE_PREFIXES[event_class])
        for key in bucket.list(prefix=get_prefix):
            m = file_regex.match(key.key)
            if m is not None:
                created_at = m.group('created_at')
                created_at_ts = datetime.strptime(created_at, '%Y%m%d%H%M%S%f')
                created_at_ts = UtcDateTime(created_at_ts)
                uploaded_at = key.last_modified
                uploaded_at_ts = UtcDateTime(uploaded_at)
                if is_client_file_in_date_range(logger, created_at_ts, before, after):
                    first_in_date_range_file_processed = True
                    extract_events(events, key, event_type, threadid, after, before, deviceid,
                                           userid, event_class, search, uploaded_at_ts, m)
                else:
                    # keep storing the prev key till the first in date range file is processed
                    if not first_in_date_range_file_processed:
                        logger.debug("File uploaded at %s is being kept as prev", uploaded_at_ts)
                        prev_key = key
                        prev_key_uploaded_at_ts = uploaded_at_ts

        # process first file last - events are sorted later
        if prev_key:
            m = file_regex.match(prev_key.key)
            logger.debug("File uploaded at %s should be processed", prev_key_uploaded_at_ts)
            ev = extract_events(events, prev_key, event_type, threadid, after, before, deviceid,
                                   userid, event_class, search, prev_key_uploaded_at_ts, m)
    if logger:
        logger.debug("Found %d %s events.", len(events), event_class)
    return events

def extract_events(events, key, event_type, threadid, after, before, deviceid,
                   userid, event_class, search, uploaded_at_ts, m):
    file_content = zlib.decompress(key.get_contents_as_string(), 16+zlib.MAX_WBITS)
    for line in file_content.splitlines():
        ev = json.loads(line)
        if event_type and 'event_type' in ev and ev['event_type'] != event_type:
            continue
        if threadid > 0:
            if 'thread_id' not in ev or ev['thread_id'] != threadid:
                continue
        timestamp = UtcDateTime(ev['timestamp'])
        if not (timestamp.datetime >= after.datetime and timestamp.datetime < before.datetime):
            continue
        if search != '':
            search_regex=re.compile(search)
            sm = search_regex.search(line)
        else:
            sm = None
        if search == '' or sm is not None:
            ev['timestamp'] = timestamp
            if 'module' not in ev:
                ev['module'] = 'client'
            if deviceid == '':
                ev['device_id'] = m.group('device_id')
            else:
                ev['device_id'] = deviceid
            if userid == '':
                ev['user_id'] = m.group('user_id')
            else:
                ev['user_id'] = userid
            ev['uploaded_at'] = uploaded_at_ts
            if 'ui_integer' not in ev and 'ui_long' in ev:
                ev['ui_integer'] = ev['ui_long']
            if 'event_type' not in ev:
                ev['event_type'] = event_class
            # TODO move this out appropriately
            if 'counter_start' in ev:
                ev['counter_start'] = UtcDateTime(ev['counter_start'])
            if 'counter_end' in ev:
                ev['counter_end'] = UtcDateTime(ev['counter_end'])
            events.append(ev)

def is_pinger_file_in_date_range(logger, uploaded_at_ts, before, after, prev_file_uploaded_at_ts):
    if uploaded_at_ts.datetime >= after.datetime and uploaded_at_ts.datetime < before.datetime:
        logger.debug("File uploaded at %s is between start/end[%s - %s]", uploaded_at_ts, before, after)
        return True
    else:
        if prev_file_uploaded_at_ts and prev_file_uploaded_at_ts.datetime >= after.datetime and prev_file_uploaded_at_ts.datetime < before.datetime:
            logger.debug("Prev File uploaded at %s is between %s and %s. So the file at %s should also be included", prev_file_uploaded_at_ts, before, after, uploaded_at_ts)
            return True
        elif not prev_file_uploaded_at_ts and uploaded_at_ts.datetime >= before.datetime:
            logger.debug("File uploaded at %s is after %s. But there is no prev files yet, so the file should be included", uploaded_at_ts, before)
            return True
        else:
            return False

def is_client_file_in_date_range(logger, created_at_ts, before, after):
    if created_at_ts.datetime >= after.datetime and created_at_ts.datetime < before.datetime:
        logger.debug("File created at %s is between start/end[%s - %s]", created_at_ts, before, after)
        return True
    else:
        return False

def get_T3_date_prefixes(after, before):
    from datetime import timedelta
    prefixes = []
    delta = before.datetime.date() - after.datetime.date()
    for i in range(delta.days + 1):
        d = after.datetime.date() + timedelta(days=i)
        prefixes.append("%s" % (d.strftime('%Y%m%d')))
    return prefixes

def get_latest_device_info_event(conn, bucket_name, userid, deviceid, after, before, logger=None):
    device_info_list = get_client_events(conn, bucket_name, userid, deviceid, after, before, 'DEVICEINFO', '', logger=logger)
    if len(device_info_list) > 0:
        return device_info_list[-1]
    else:
        # widen the search :-( a week before
        for i in range(7):
            before = UtcDateTime(str(after))
            from datetime import timedelta
            after = before.datetime - timedelta(days=1)
            logger.debug("Check from %s to %s for device info", after, before)
            device_info_list = get_client_events(conn, bucket_name, userid, deviceid, UtcDateTime(str(after)), UtcDateTime(str(before)), 'DEVICEINFO', '', logger=logger)
            if len(device_info_list) > 0:
                return device_info_list[-1]
        return None


def get_trouble_ticket_events(conn, bucket_name, logger):
    logger.info("Checking for new trouble tickets in %s", bucket_name)
    assert isinstance(conn, S3Connection)
    bucket = conn.get_bucket(bucket_name)
    events = []
    for key in bucket.list():
        file_content = zlib.decompress(key.get_contents_as_string(), 16+zlib.MAX_WBITS)
        for line in file_content.splitlines():
            ev = json.loads(line)
            ev['event_type'] = 'SUPPORT'
            ev['timestamp'] = UtcDateTime(ev['timestamp'])
            if 'module' not in ev:
                ev['module'] = 'client'
                ev['device_id'] = ev['client']
                ev['user_id'] = ev['user']
                ev['key_name'] = key.name
            events.append(ev)
    logger.info("Found %d trouble tickets", len(events))
    return events

def delete_trouble_ticket(conn, bucket_name, key_name, logger):
    logger.info("Deleting trouble ticket %s", key_name)
    assert isinstance(conn, S3Connection)
    bucket = conn.get_bucket(bucket_name)
    key = bucket.delete_key(key_name)
    return key
