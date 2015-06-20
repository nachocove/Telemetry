# Copyright 2014, NachoCove, Inc
import json
import re
import zlib
from boto.s3.connection import S3Connection
from misc.utc_datetime import UtcDateTime
from datetime import datetime
import hashlib

T3_EVENT_CLASS_FILE_PREFIXES = {
         'ALL': ['PROTOCOL','LOG', 'COUNTER', 'STATISTICS2','UI', 'SUPPORT', 'DEVICEINFO','PINGER', 'SAMPLES'],
         'REDSHIFT': ['PROTOCOL','LOG', 'COUNTER', 'STATISTICS2','UI', 'DEVICEINFO', 'SAMPLES'],
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
}

def get_pinger_events(conn, bucket_name, userid, deviceid, after, before, search, logger=None):
    logger.info("Getting events of class PINGER...")
    assert isinstance(conn, S3Connection)
    bucket = conn.get_bucket(bucket_name)
    #sample key
    #c6ae00d0-e259-4bfc-903d-5b6bc62cd651-dev-pinger
    #20150529/plog-20150529063152823.gz
    events = []
    nm=0
    prev_file_uploaded_at_ts = None
    for date_prefix in get_T3_date_prefixes(after, before):
        get_prefix = date_prefix
        logger.info("get_prefix is %s" % get_prefix)
        file_regex = re.compile(r'.*/%s-(?P<uploaded_at>[0-9]+).gz' % T3_EVENT_CLASS_FILE_PREFIXES['PINGER'])

        search_regex=re.compile(search)
        for key in bucket.list(prefix=get_prefix):
            m = file_regex.match(key.key)
            if m is not None:
                uploaded_at = m.group('uploaded_at')
                uploaded_at_ts = datetime.strptime(uploaded_at, '%Y%m%d%H%M%S%f')
                uploaded_at_ts = UtcDateTime(uploaded_at_ts)
                if file_in_date_range(logger, uploaded_at_ts, before, after, prev_file_uploaded_at_ts):
                    logger.info("File uploaded at %s is between %s and %s", uploaded_at_ts, before, after)
                    file_content = zlib.decompress(key.get_contents_as_string(), 16+zlib.MAX_WBITS)
                    for line in file_content.splitlines():
                        ev = json.loads(line)
                        timestamp = UtcDateTime(ev['timestamp'])
                        if not (timestamp.datetime >= after.datetime and timestamp.datetime < before.datetime):
                            nm+=1
                            continue
                        if search != '':
                            sm = search_regex.search(line)
                        else:
                            sm = None
                        if search == '' or sm is not None:
                            if userid and ev['client'] != userid:
                                nm+=1
                                continue
                            if deviceid and ev['device'] != deviceid:
                                nm+=1
                                continue
                            ev['device_id'] = ev['device']
                            ev['user_id'] = ev['client']
                            ev['thread_id'] = ""
                            ev['timestamp'] = timestamp
                            ev['uploaded_at'] = uploaded_at_ts
                            events.append(ev)
                    prev_file_uploaded_at_ts = uploaded_at_ts
    if logger:
        logger.debug("Found %d PINGER events. not matched %s", len(events), nm)
    return events

def get_client_events(conn, bucket_name, userid, deviceid, after, before, type, search, logger=None):
    logger.info("Getting events of class %s for userid %s deviceid %s" % (type, userid, deviceid))
    assert isinstance(conn, S3Connection)
    bucket = conn.get_bucket(bucket_name)
    #sample key
    #c6ae00d0-e259-4bfc-903d-5b6bc62cd651-dev-telemetry
    #20150529/bc88aa25/us-east-1:40cc3511-c031-4f67-ba6e-0bbf26e9403d/Nchob8e8562d6b22/NachoMail/log-20150529063152823.gz
    if userid:
        client_prefix = '/' + hashlib.sha256(userid).hexdigest()[0:8] + '/' + userid
        if deviceid:
            client_prefix += '/' + deviceid + '/NachoMail/' + T3_EVENT_CLASS_FILE_PREFIXES[type]
    else:
        client_prefix = ''
    events = []
    nm=0
    prev_file_uploaded_at_ts = None
    for date_prefix in get_T3_date_prefixes(after, before):
        get_prefix = date_prefix + client_prefix
        logger.info("get_prefix is %s" % get_prefix)
        userid_regex = '\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+'
        if not userid:
            if deviceid:
                file_regex = re.compile(r'.*/(?P<user_id>%s)/%s/NachoMail/%s-(?P<uploaded_at>[0-9]+).gz' % (userid_regex, deviceid, T3_EVENT_CLASS_FILE_PREFIXES[type]))
            else:
                file_regex = re.compile(r'.*/(?P<user_id>%s)/(?P<device_id>Ncho\w+)/NachoMail/%s-(?P<uploaded_at>[0-9]+).gz' % (userid_regex, T3_EVENT_CLASS_FILE_PREFIXES[type]))
        else:
            if not deviceid:
                file_regex = re.compile(r'.*/(?P<device_id>Ncho\w+)/NachoMail/%s-(?P<uploaded_at>[0-9]+).gz' % T3_EVENT_CLASS_FILE_PREFIXES[type])
            else:
                file_regex = re.compile(r'.*%s-(?P<uploaded_at>[0-9]+).gz' % T3_EVENT_CLASS_FILE_PREFIXES[type])

        search_regex=re.compile(search)
        for key in bucket.list(prefix=get_prefix):
            m = file_regex.match(key.key)
            if m is not None:
                uploaded_at = m.group('uploaded_at')
                uploaded_at_ts = datetime.strptime(uploaded_at, '%Y%m%d%H%M%S%f')
                uploaded_at_ts = UtcDateTime(uploaded_at_ts)
                if file_in_date_range(logger, uploaded_at_ts, before, after, prev_file_uploaded_at_ts):
                    logger.info("File uploaded at %s is between %s and %s", uploaded_at_ts, before, after)
                    file_content = zlib.decompress(key.get_contents_as_string(), 16+zlib.MAX_WBITS)
                    for line in file_content.splitlines():
                        ev = json.loads(line)
                        timestamp = UtcDateTime(ev['timestamp'])
                        if not (timestamp.datetime >= after.datetime and timestamp.datetime < before.datetime):
                            nm+=1
                            continue
                        if search != '':
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
                                ev['event_type'] = type
                            # TODO move this out appropriately
                            if 'counter_start' in ev:
                                ev['counter_start'] = UtcDateTime(ev['counter_start'])
                            if 'counter_end' in ev:
                                ev['counter_end'] = UtcDateTime(ev['counter_end'])
                            events.append(ev)
                    prev_file_uploaded_at_ts = uploaded_at_ts

    if logger:
        logger.debug("Found %d %s events. not matched %s", len(events), type, nm)
    return events

def file_in_date_range(logger, uploaded_at_ts, before, after, prev_file_uploaded_at_ts):
    if uploaded_at_ts.datetime >= after.datetime and uploaded_at_ts.datetime < before.datetime:
        return True
    else:
        if prev_file_uploaded_at_ts and prev_file_uploaded_at_ts.datetime >= after.datetime and prev_file_uploaded_at_ts.datetime < before.datetime:
            logger.info("Prev File uploaded at %s is between %s and %s. So the file at %s should also be included", prev_file_uploaded_at_ts, before, after, uploaded_at_ts)
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
