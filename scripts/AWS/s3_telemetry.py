# Copyright 2014, NachoCove, Inc
import json
import os
import re
import zlib
from boto.s3.connection import S3Connection
from misc.utc_datetime import UtcDateTime

def get_s3_events(conn, bucket_name, prefix, event_prefix, after, before, logger=None):
    assert isinstance(conn, S3Connection)
    bucket = conn.get_bucket(bucket_name)
    # make sure to have the slash at the end, but not at the start
    prefix = os.path.join(prefix, '').lstrip('/')
    file_regex = re.compile(r'%s%s--(?P<start>[0-9\-:TZ\.]+)--(?P<end>[0-9\-:TZ\.]+)\.json(?P<ext>.*)' % (prefix, event_prefix))
    events = []
    last_item = None
    done = False
    for get_prefix in get_prefixes(prefix, event_prefix, after, before):
        while not done:
            keys = bucket.get_all_keys(prefix=get_prefix, maxkeys=1000, marker=last_item)
            if not keys:
                break
            for key in keys:
                last_item = key.key
                m = file_regex.match(key.key)
                if m is not None:
                    start = UtcDateTime(m.group('start'))
                    end = UtcDateTime(m.group('end'))

                    # does it fall within the before and after range?
                    if (start.datetime >= after.datetime and start.datetime < before.datetime) or \
                            (end.datetime >= after.datetime and end.datetime < before.datetime):
                        if m.group('ext') == '.gz':
                            #http://stackoverflow.com/questions/1543652/python-gzip-is-there-a-way-to-decompress-from-a-string/18319515#18319515
                            json_str = zlib.decompress(key.get_contents_as_string(), 16+zlib.MAX_WBITS)
                        elif m.group('ext') == '':
                            json_str = key.get_contents_as_string()
                        else:
                            raise Exception("unknown extension %s" % m.group('ext'))
                        for ev in json.loads(json_str):
                            timestamp = UtcDateTime(ev['timestamp'])
                            if not (timestamp.datetime >= after.datetime and timestamp.datetime < before.datetime):
                                continue
                            ev['thread_id'] = ""
                            ev['timestamp'] = timestamp
                            ev['uploaded_at'] = UtcDateTime(ev['uploaded_at'])
                            events.append(ev)

                    # did we pass the last possible time?
                    elif start.datetime > before.datetime:
                        # this relies on the fact that s3 sorts the returned keys in alphabetical order, and
                        # the files with timestamp are properly sorted. If that no longer holds, we'll
                        # miss files, so we'll need a different strategy.
                        done = True
                        break
        if done:
            break
    if logger:
        logger.debug("Found %d events", len(events))
    return events

def get_prefixes(prefix, event_prefix, after, before):
    #prefixes.append("%s%s--%s" % (prefix, event_prefix, (after.datetime - timedelta(hours=1)).strftime('%Y-%m-%dT%H')))
    #prefixes.append("%s%s--%s" % (prefix, event_prefix, after.datetime.strftime('%Y-%m-%dT%H:%M')))
    return ["%s%s--" % (prefix, event_prefix)]
