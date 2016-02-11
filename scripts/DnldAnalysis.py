# Copyright 2014, NachoCove, Inc
import argparse
import json
import logging
import re
import sys

from datetime import timedelta

from AWS.redshift_handler import create_db_conn, select


def json_config(file_name):
    with open(file_name) as data_file:
        json_data = json.load(data_file)
    return json_data


sql1_where_options = ["message like 'Starting DnldEmailBodyCmd%%McPending%%'", ]
sql1 = "select distinct split_part(message, '/', 2),device_id,message from alpha_nm_log where %s order by message;"
sql2 = "select timestamped,device_id,message from alpha_nm_log where %s order by timestamped;"
sql2_where_options = ["device_id='%(device)s'"]
sql2_message_or_options = ["message like 'McPending%%/%(guid)s/EmailBodyDownload%%'",
                           ]

def main():
    parser = argparse.ArgumentParser(description='Download Analysis tool')
    parser.add_argument('--config', required=True, type=json_config, metavar="config_file",
                        help='the config(json) file for the deployment', )
    parser.add_argument('-d', '--debug',
                        help='Debug',
                        action='store_true',
                        default=False)
    parser.add_argument("--device", type=str, default=None)
    parser.add_argument("--after", type=str, default=None)
    parser.add_argument("--before", type=str, default=None)
    parser.add_argument("--all-results", action='store_true',
                        default=False)
    default_download_seconds = 3
    parser.add_argument("--seconds", help="allowable seconds to declare a successful download (default %s)" % default_download_seconds, default=default_download_seconds, type=int)

    args = parser.parse_args()
    if not args.after:
        args.print_help()
        sys.exit(1)

    config = args.config

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)

    logger.debug("Creating connection...")
    conn = create_db_conn(logger, config["db_config"])
    conn.autocommit = False
    cursor = conn.cursor()
    if args.after:
        sql1_where_options.append("timestamped >= '%(after)s'")
        sql2_where_options.append("timestamped >= '%(after)s'")
    if args.before:
        sql1_where_options.append("timestamped <= '%(before)s'")
        sql2_where_options.append("timestamped <= '%(before)s'")
    if args.device:
        sql1_where_options.append("device_id='%(device)s'")

    q = (sql1 % " and ".join(sql1_where_options)) % {'after': args.after,
                                                     'before': args.before,
                                                     'device': args.device,
                                                    }
    logger.debug("Query: \"%s\"", q)
    rows = select(logger, cursor, q)
    if rows:
        for row in rows:
            guid = row[0]
            device = row[1]
            logger.debug("Looking for %s", ",".join([str(x) for x in row]))
            cursor1 = conn.cursor()
            if len(sql2_message_or_options) > 1:
                sql2_where_options.append("(%s)" % " or ".join(sql2_message_or_options))
            else:
                sql2_where_options.extend(sql2_message_or_options)

            q = (sql2 % " and ".join(sql2_where_options)) % {'after': args.after,
                                                             'before': args.before,
                                                             'guid': guid,
                                                             'device': device}
            logger.debug("   Query: \"%s\"", q)
            guid_rows = select(logger, cursor1, q)
            started = None
            finished = None
            result = []
            for guid_row in guid_rows:
                logger.debug("   guid row for %s", ",".join([str(x) for x in guid_row]))
                if started is None:
                    started = guid_row[0]
                else:
                    finished = guid_row[0]
                match = re.match(r'.*ResolveAs(?P<result>.+)$', guid_row[2])
                if match:
                    result.append(match.group('result'))
            took = finished-started
            if args.all_results or "Success" not in result or took > timedelta(seconds=args.seconds):
                d = {'device': device,
                     'took': took,
                     'started': started,
                     'finished': finished,
                     'results': ",".join(result),
                     'guid': guid,
                     }
                logger.info("%(device)s: Download %(guid)s took: %(took)s (started %(started)s, finished %(finished)s), results: %(results)s" % d)

    exit(0)

if __name__ == '__main__':
    main()
