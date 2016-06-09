# Copyright 2014, NachoCove, Inc
import json
import os
import re
import zlib
from boto.s3.connection import S3Connection
from misc.utc_datetime import UtcDateTime

def create_s3_conn(aws_access_key_id, aws_secret_access_key, security_token=None, host='s3-us-west-2.amazonaws.com', port=443, debug=False):
    return S3Connection(host=host,
                        port=port,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_access_key_id=aws_access_key_id,
                        security_token=security_token,
                        is_secure=True,
                        debug=2 if debug else 0)

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


def delete_s3_events(conn, bucket_name, prefixes, after, before, logger=None):
    assert isinstance(conn, S3Connection)
    bucket = conn.get_bucket(bucket_name)
    for prefix in prefixes:
        last_item = None
        logger.debug("Looking for key prefix %s/%s", bucket_name, prefix)
        while True:
            keys = bucket.get_all_keys(prefix=prefix, maxkeys=1000, marker=last_item)
            if not keys:
                break
            for key in keys:
                logger.debug("deleting key %s" % key.key)
            result = bucket.delete_keys(keys)
            if result.errors:
                logger.error("Could not delete keys:\n%s", "\n".join(["%s: %s" % (x.message, x.key) for x in result.errors]))
                return False
    return True
