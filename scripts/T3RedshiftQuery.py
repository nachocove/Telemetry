# Copyright 2014, NachoCove, Inc
import argparse
import json
import logging

import sys

from AWS.db_reports import select
from AWS.redshift_handler import create_db_conn


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
    parser.add_argument("query", help="Query", type=str)

    args = parser.parse_args()
    config = args.config

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)

    logger.debug("Creating connection...")
    conn = create_db_conn(logger, config["db_config"])
    conn.autocommit = False
    cursor = conn.cursor()
    logger.debug("Query: \"%s\"", args.query)
    rows = select(logger, cursor, args.query)
    if rows:
        for row in rows:
            logger.info(",".join([str(x) for x in row]))

    exit()

if __name__ == '__main__':
    main()
