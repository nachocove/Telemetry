# Copyright 2016, NachoCove, Inc
import argparse
import json
import logging
import re
import sys

from datetime import timedelta, datetime

from AWS.redshift_handler import create_db_conn, select


def json_config(file_name):
    with open(file_name) as data_file:
        json_data = json.load(data_file)
    return json_data

sql1_where_options = []
startedWhere = "message like 'NcTask %% started on %%'"
completedWhere = "message like 'NcTask %% completed after %%'"

sql1 = "select message from %(project)s_nm_log where %(where)s order by timestamped;"

def main():
    parser = argparse.ArgumentParser(description='Task Analysis tool')
    parser.add_argument('--config', required=True, type=json_config, metavar="config_file",
                        help='the config(json) file for the deployment', )
    parser.add_argument('-d', '--debug',
                        help='Debug',
                        action='store_true',
                        default=False)
    parser.add_argument('-v', '--verbose',
                        help='Debug',
                        action='store_true',
                        default=False)
    parser.add_argument("--device", type=str, default=None)
    parser.add_argument("--after", type=str, default=None)
    parser.add_argument("--before", type=str, default=None)

    args = parser.parse_args()
    if not args.after:
        parser.print_help()
        sys.exit(1)
    if not args.before:
        parser.print_help()
        sys.exit(1)

    config = args.config
    project = config['general_config']['project']

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
    if args.before:
        sql1_where_options.append("timestamped <= '%(before)s'")
    if args.device:
        sql1_where_options.append("device_id='%(device)s'")

    q = (sql1 % {'project': project, 'where': " and ".join(sql1_where_options + [startedWhere])}) % {'after': args.after,
                                                                                                     'before': args.before,
                                                                                                     'device': args.device,
                                                                                                     }
    logger.debug("Query: \"%s\"", q)
    not_finished = []
    startedRows = select(logger, cursor, q)
    if startedRows:
        logger.debug("Found %d tasks to look for", len(startedRows))
        q = (sql1 % {'project': project, 'where': " and ".join(sql1_where_options + [completedWhere])}) % {
            'after': args.after,
            'before': args.before,
            'device': args.device,
            }
        completedRows = select(logger, cursor, q)
        for row in startedRows:
            msg = row[0]
            msgMatch = re.match(r'NcTask (?P<threadName>.*) started on (?P<threadType>.*) (?P<threadId>\d+), (?P<running>\d+) running', msg)
            if not msgMatch:
                raise Exception("No match")
            logger.debug("Looking for Task Completion of %s", msgMatch.group("threadName"))
            completedRe = re.compile(r'NcTask %s completed after (?P<msec>[\d\,]+) msec.' % msgMatch.group("threadName"))
            found_match = None
            for completed in completedRows:
                completedMatch = completedRe.match(completed[0])
                if completedMatch:
                    found_match = completed[0]
                    break
            if found_match:
                logger.debug("  Task Completed: %s" % found_match)
                continue
            else:
                if args.verbose or args.debug:
                    logger.info("  No completed rows for %s", msgMatch.group("threadName"))
                not_finished.append(msg)
                continue
        if not args.verbose:
            logger.info("Did not find completed task rows for:\n%s", "\n".join(not_finished))
    else:
        logger.info("No rows found")


    exit(0)

if __name__ == '__main__':
    main()
