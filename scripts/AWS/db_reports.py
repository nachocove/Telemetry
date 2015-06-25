__author__ = 'azimo'
import sys
import time
import traceback
import boto
import argparse
import json
from datetime import timedelta, datetime, date
from pprint import pprint
from boto.exception import S3ResponseError, EC2ResponseError, BotoServerError
from AWS.redshift_handler import create_db_conn
from misc.utc_datetime import UtcDateTime
from monitors.monitor_base import get_client_telemetry_link
from analytics.token import TokenList, WhiteSpaceTokenizer
from analytics.cluster import Clusterer

def parse_dates(args):
    if not args.start and not args.end and args.period == "daily": # run yesterday's report
        start = UtcDateTime(datetime.fromordinal((datetime.now() - timedelta(1)).toordinal()))
        end = UtcDateTime(datetime.fromordinal((datetime.now()).toordinal())-timedelta(seconds=1))
    else:
        if not args.start and args.period == "daily": # only support daily right now
            start = UtcDateTime(datetime.fromordinal((UtcDateTime(args.end).datetime - timedelta(1)).toordinal()))
        else:
            start = UtcDateTime(args.start)
        if not args.end and args.period == "daily":
            end = UtcDateTime(datetime.fromordinal((UtcDateTime(args.start).datetime + timedelta(1)).toordinal()))
        else:
            end = UtcDateTime(args.end)
    return start, end

def classify_log_events(events):
    # Cluster log messages
    clusterer = Clusterer()
    tokenizer = WhiteSpaceTokenizer()
    for log in events:
        # We cluster using only the first line of the message
        message = log['message'].split('\n')[0]
        token_list = TokenList(tokenizer.process(message))
        clusterer.add(token_list)

    # Generate the report dictionary
    clustered_events = []
    ccount = 0
    for cluster in clusterer.clusters:
        message = unicode(cluster.pattern)
        if len(message) > 70:
            new_message = message[:76] + ' ...'
        else:
            new_message = message
        ccount += len(cluster)
        clustered_events.append({"count":len(cluster), "message":new_message})
    clustered_events_sorted = sorted(clustered_events, key=lambda x: x['count'], reverse=True)
    return clustered_events_sorted

def select(logger, cursor, sql_st):
    # need a connection with dbname=<username>_db
    try:
        # retrieving all tables in my search_path
        x= cursor.execute(sql_st)
    except Exception as err:
        logger.error(err)
    rows = cursor.fetchall()
    return rows

def log_report(logger, project, config, start, end):
    host="http://localhost:8081/"
    summary = {'start':start, 'end':end}
    summary['period'] = str(end.datetime - start.datetime)
    summary['total_hours'] = (end.datetime-start.datetime).days*24 + (end.datetime-start.datetime).seconds/3600
    startForRS = start.datetime.strftime('%Y-%m-%d %H:%M:%S')
    endForRS = end.datetime.strftime('%Y-%m-%d %H:%M:%S')
    error_list = []
    warning_list = []
    logger.info("Creating connection...")
    conn = create_db_conn(logger, config["db_config"])
    conn.autocommit = False
    cursor = conn.cursor()
    try:
        logger.info("Selecting error counts from logs...")
        #TODO fix %s in SQL statements
        sql_statement = "select event_type, user_id, device_id, timestamped, message from %s_nm_log " \
                        "where event_type='ERROR' and " \
                        "timestamped >= '%s' and timestamped <= '%s'" % (project, startForRS, endForRS)
        logger.info(sql_statement)
        rows = select(logger, cursor, sql_statement)
        for row in rows:
            telemetry_link = get_client_telemetry_link(project, row[2], row[3], host=host, isT3=True)
            error_list.append({"event_type": row[0], "user_id": row[1], "device_id": row[2], "timestamp": UtcDateTime(row[3]),
                                 "message": row[4], 'telemetry_link': telemetry_link})
            logger.info("%s successful, Read %s rows", cursor.statusmessage, cursor.rowcount)
        logger.info("Selecting warning counts from logs...")
        sql_statement = "select event_type, user_id, device_id, timestamped, message from %s_nm_log " \
                        "where event_type='WARN' and " \
                        "timestamped >= '%s' and timestamped <= '%s'" % (project, startForRS, endForRS)
        logger.info(sql_statement)
        rows = select(logger, cursor, sql_statement)
        for row in rows:
            telemetry_link = get_client_telemetry_link(project, row[2], row[3], host=host, isT3=True)
            warning_list.append({"event_type": row[0], "user_id": row[1], "device_id": row[2], "timestamp": UtcDateTime(row[3]),
                                 "message": row[4], 'telemetry_link': telemetry_link})
        logger.info("%s successful, Read %s rows", cursor.statusmessage, cursor.rowcount)
        logger.info("Selecting total counts from logs...")
        sql_statement = "select distinct count(*), event_type from %s_nm_log " \
                        "where timestamped >= '%s' and timestamped <= '%s'" \
                        "group by event_type order by count desc"% (project, startForRS, endForRS)
        logger.info(sql_statement)
        rows = select(logger, cursor, sql_statement)
        total_count = 0
        for row in rows:
            summary[row[1]] = row[0]
            if row[0] > 0:
                summary[row[1] + '_rate'] = float(row[0])/summary['total_hours']
            else:
                summary[row[1] + '_rate'] = 0
            total_count += row[0]
        summary['event_count'] = total_count
        if total_count > 0:
            summary['event_rate'] = float(total_count)/summary['total_hours']
        else:
            summary['event_rate'] = 0
        logger.info("%s successful, Read %s rows", cursor.statusmessage, cursor.rowcount)
        return summary, error_list, warning_list
    except Exception as err:
        logger.error(err)
        logger.error(traceback.format_exc())
    cursor.close()
    conn.close()

def execute_sql(logger, project, config, sql_query):
    logger.info("Creating connection...")
    conn = create_db_conn(logger, config["db_config"])
    conn.autocommit = False
    cursor = conn.cursor()
    logger.info("Running custom sql query...")
    #TODO fix %s in SQL statements
    sql_statement = sql_query
    logger.info(sql_statement)
    rows = []
    col_names = ""
    error = ""
    try:
        status= cursor.execute(sql_statement)
        num_fields = len(cursor.description)
        col_names = [i[0] for i in cursor.description]
        rows = cursor.fetchall()
    except Exception as err:
        error = err
        logger.error(err)
        logger.error(traceback.format_exc())
    cursor.close()
    conn.close()
    return error, col_names, rows