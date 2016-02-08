# Copyright 2014, NachoCove, Inc
import argparse
import json
import logging
import re

import sys

from AWS.redshift_handler import create_db_conn, select

sql1_where_options = ["message like 'fd%%%(substring)s%%'",]
sql1 = "select distinct split_part(message, ': ', 2),device_id from alpha_nm_log where %s order by message;"
sql2 = "select timestamped,device_id,message from alpha_nm_log where %s order by timestamped;"
sql2_where_options = ["message like 'fd%%%(guid)s%%'",
                      "device_id='%(device)s'"]


def json_config(file_name):
    with open(file_name) as data_file:
        json_data = json.load(data_file)
    #pprint(json_data)
    return json_data


def main():
    parser = argparse.ArgumentParser(description='T3 RedShift Loader')
    parser.add_argument('--config', required=True, type=json_config, metavar = "config_file",
                   help='the config(json) file for the deployment', )
    parser.add_argument('-d', '--debug',
                              help='Debug',
                              action='store_true',
                              default=False)
    parser.add_argument("--device", type=str, default=None)
    parser.add_argument("--after", type=str, default=None)
    parser.add_argument("--before", type=str, default=None)
    parser.add_argument("--fdpathss", help="The fd-path substring to look for", type=str, default="wbxml-stream")

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

    q = (sql1 % " and ".join(sql1_where_options)) % {'after': args.after, 'before': args.before, 'device': args.device, 'substring': args.fdpathss}
    logger.debug("Query: \"%s\"", q)
    dup_guids = {}
    rows = select(logger, cursor, q)
    if rows:
        for row in rows:
            logger.debug("Considering %s", ",".join([str(x) for x in row]))
            fd = None
            orig_row = None
            cursor1 = conn.cursor()
            dups = {}
            guid = row[0].split('/')[-1].split("wbxml")[0]
            device = row[1]
            q = (sql2 % " and ".join(sql2_where_options)) % {'after': args.after, 'before': args.before, 'guid': guid, 'device': device}
            logger.debug("   Query: \"%s\"", q)
            fd_rows = select(logger, cursor1, q)
            for fdrow in fd_rows:
                message = fdrow[2]
                logger.debug("   Considering %s", ",".join([str(x) for x in fdrow]))
                match = re.match(r'^fd (?P<fd>\d+): .*$', message)
                if match:
                    fdnew = int(match.group('fd'))
                    if not fd:
                        fd = fdnew
                        orig_row = fdrow
                    elif fd != fdnew and fdnew not in dups:
                        dups[fdnew] = "Different FD: " + ",".join([str(x) for x in fdrow])

            if dups:
                key = "%s,%s" % (device,guid)
                dups[fd] = "Original FD: " + ",".join([str(x) for x in orig_row])
                logger.info("Key %s open multiple times: %s", key, dups.keys())
                if key in dup_guids:
                    logger.error("Did distinct not work?")
                dup_guids[key] = dups

    logger.info("All results: %s", json.dumps(dup_guids, indent=4))

    exit(0)

if __name__ == '__main__':
    main()
