__author__ = 'azimo'
import sys
import time
import traceback
import boto
import boto.vpc
import argparse
import json
from datetime import timedelta, datetime, date
from pprint import pprint
from boto.s3.key import Key
import boto.ec2
from boto.exception import S3ResponseError, EC2ResponseError, BotoServerError
from boto.s3.connection import S3Connection
from misc.emails import Email, EmailServer
from misc.html_elements import *
from monitors.monitor_base import Summary
from django.conf import settings
from misc.utc_datetime import UtcDateTime
import logging
import psycopg2
from django.shortcuts import render, render_to_response
from django.template import Template, Context
from django.template import loader
from django.template.loader import render_to_string
from django.core.mail import EmailMultiAlternatives
from django.utils.html import strip_tags
import os
import codecs
try:
    from cloghandler import ConcurrentRotatingFileHandler as RFHandler
except ImportError:
    from logging.handlers import RotatingFileHandler as RFHandler

class DateTimeAction(argparse.Action):
    """
    This class parses the ISO-8601 UTC time given in --start and --end.
    """
    def __call__(self, parser, namespace, value, option_string=None):
        if (option_string != '--start') and (option_string != '--end'):
            raise ValueError('unexpected option %s with datetime ' % option_string)
        if (option_string == '--start') and ('last' == value):
            setattr(namespace, self.dest, 'last')
        elif (option_string == '--end') and (value.startswith('now')):
            setattr(namespace, self.dest, UtcDateTime(value))
        else:
            try:
                setattr(namespace, self.dest, UtcDateTime(value))
            except Exception:
                raise argparse.ArgumentError(self, "not a valid Date-time argument: %s" % value)

def datetime_tostr(iso_datetime):
    """
    This function returns a string from a UtcDateTime object that is good for
    usage as part of file paths.
    """
    datetime_str = str(iso_datetime)
    return datetime_str.replace(':', '_').replace('-', '_').replace('.', '_')

def last_sunday():
    today = date.today().toordinal()
    sunday = today - (today % 7)
    return UtcDateTime(datetime.fromordinal(sunday))

def yesterday_midnight():
    return UtcDateTime(datetime.fromordinal((date.today() - timedelta(1)).toordinal()))

def today_midnight():
    return UtcDateTime(datetime.fromordinal(date.today().toordinal()))

def guess_last(period):
    ret = None
    if isinstance(period, (unicode, str)):
        if period == 'weekly':
            ret = last_sunday()
        elif period == 'daily':
            ret = yesterday_midnight()
        else:
            try:
                ret = UtcDateTime(datetime.now()-timedelta(seconds=int(period)))
            except ValueError:
                pass
    elif isinstance(period, (int, long)):
        ret = UtcDateTime(datetime.now()-timedelta(seconds=period))
    elif isinstance(period, timedelta):
        ret = UtcDateTime(datetime.now()-period)

    if not ret:
        raise ValueError('Unknown value %s for period' % period)
    return ret

def period_to_seconds(period):
    ret = None
    if isinstance(period, (unicode, str)):
        if period == 'weekly':
            ret = 7*24*60*60
        elif period == 'daily':
            ret = 60*60*24
        elif period == 'hourly':
            ret = 60*60
        else:
            try:
                ret = int(period)
            except ValueError:
                pass
    elif isinstance(period, (int, long)):
        ret = period
    if not ret:
        raise ValueError('Unknown value %s for period' % period)
    return ret


# get region from region_name
def get_region(region_name):
    for region in boto.ec2.regions():
        if region_name == region.name:
            return region

def create_conn(logger, db_config):
    conn=None
    try:
        conn=psycopg2.connect(dbname=db_config['dbname'], host=db_config['host'],
                              port=db_config['port'], user=db_config['user'], password=db_config['pwd'])
    except Exception as err:
        logger.error(err)
    return conn

def select(logger, cursor, sql_st):
    # need a connection with dbname=<username>_db
    try:
        # retrieving all tables in my search_path
        x= cursor.execute(sql_st)
    except Exception as err:
        logger.error(err)
    rows = cursor.fetchall()
    return rows

# cleanup
def cleanup(logger, config):
    logger.info("Cleaning up...")
    exit(-1)

# load json config
def json_config(file_name):
    with open(file_name) as data_file:
        json_data = json.load(data_file)
    #pprint(json_data)
    return json_data

def get_user_device_prefixes(logger, config, startdt_prefix):
    aws_config = config["aws_config"]
    s3_config = config["s3_config"]
    prefixes =[]
    conn = S3Connection(host='s3-us-west-2.amazonaws.com',
                                                         port=443,
                                                         aws_secret_access_key=aws_config["aws_secret_access_key"],
                                                         aws_access_key_id=aws_config["aws_access_key_id"],
                                                         is_secure=True,
                                                         debug=0)
    bucket_name = s3_config["client_t3_log_bucket"]
    bucket = conn.get_bucket(bucket_name)
    from boto.s3 import prefix
    for l1_prefix in bucket.list(prefix=startdt_prefix + '/', delimiter='/'):
        for l2_prefix in bucket.list(prefix=l1_prefix.name, delimiter='/'):
            for l3_prefix in bucket.list(prefix=l2_prefix.name, delimiter='/'):
                prefixes.append(l3_prefix.name + 'NachoMail')
    return prefixes

def delete_logs(logger, config):
    aws_config = config["aws_config"]
    s3_config = config["s3_config"]
    try:
        logger.info("Creating connection...")
        conn = create_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Deleting logs...")
        sql_statement="delete from clientlog"
        try:
            logger.info(sql_statement)
            cursor.execute(sql_statement)
            # we dont get back a row count, no errors means we are good
            logger.info("%s successful", cursor.statusmessage)
        except Exception as err:
            logger.error(err)
        conn.commit()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())
        cleanup(logger, config)

#upload logs
def upload_logs(logger, config, start, end):
    aws_config = config["aws_config"]
    s3_config = config["s3_config"]
    startdt_prefix = "%s" % (start.datetime.date().strftime('%Y%m%d'))
    try:
        logger.info("Creating connection...")
        conn = create_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Uploading logs...")
        for user_device_prefix in get_user_device_prefixes(logger, config, startdt_prefix):
            sql_statement="COPY clientlog FROM 's3://%s/%s/log-' \
            CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' \
            gzip \
            json 's3://%s/%s'" % (s3_config["client_t3_log_bucket"], user_device_prefix,
                                  aws_config["aws_access_key_id"], aws_config["aws_secret_access_key"],
                                  s3_config["client_t3_log_bucket"], s3_config["clientlog_jsonpath"])
            try:
                logger.info(sql_statement)
                cursor.execute(sql_statement)
                # we dont get back a row count, no errors means we are good
                logger.info("%s successful", cursor.statusmessage)
            except Exception as err:
                logger.error(err)
            conn.commit()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())
        cleanup(logger, config)

# select counts
def select_counts(logger, config, start, end):
    summary = {'start':start, 'end':end}
    summary['period'] = str(end.datetime - start.datetime)
    summary['total_hours'] = (end.datetime-start.datetime).days*24 + (end.datetime-start.datetime).seconds/3600
    error_list = []
    warning_list = []
    try:
        logger.info("Creating connection...")
        conn = create_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        try:
            logger.info("Selecting error counts from logs...")
            sql_statement = "select distinct count(*), substring(message, 0, 72) from clientlog where event_type='ERROR' group by message order by count desc"
            logger.info(sql_statement)
            rows = select(logger, cursor, sql_statement)
            for row in rows:
                error_list.append({"count":row[0], "message":row[1]})
            logger.info("%s successful, Read %s rows", cursor.statusmessage, cursor.rowcount)
            logger.info("Selecting warning counts from logs...")
            sql_statement = "select distinct count(*), substring(message, 0, 72) from clientlog where event_type='WARN' group by message order by count desc"
            logger.info(sql_statement)
            rows = select(logger, cursor, sql_statement)
            for row in rows:
                warning_list.append({"count":row[0], "message":row[1]})
            logger.info("%s successful, Read %s rows", cursor.statusmessage, cursor.rowcount)
            logger.info("Selecting total counts from logs...")
            sql_statement = "select distinct count(*), event_type from clientlog group by event_type order by count desc"
            logger.info(sql_statement)
            rows = select(logger, cursor, sql_statement)
            total_count = 0
            for row in rows:
                summary[row[1]] = row[0]
                summary[row[1] + '_rate'] = float(row[0])/summary['total_hours']
                total_count += row[0]
            summary['event_count'] = total_count
            summary['event_rate'] = float(total_count)/summary['total_hours']
            logger.info("%s successful, Read %s rows", cursor.statusmessage, cursor.rowcount)
            return summary, error_list, warning_list
        except Exception as err:
            logger.error(err)
        cursor.close()
        conn.close()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())
        cleanup(logger, config)

def parse_dates(args):
    if not args.start and args.period == "daily": # only support daily right now
        start = UtcDateTime(datetime.fromordinal((UtcDateTime(args.end).datetime - timedelta(1)).toordinal()))
    else:
        start = args.start
    if not args.end and args.period == "daily":
        end = UtcDateTime(datetime.fromordinal((UtcDateTime(args.start).datetime + timedelta(1)).toordinal()))
    else:
        end = args.end
    return start, end

def get_email_backend(email_config):
    from django.core.mail.backends.smtp import EmailBackend
    server = email_config['smtp_server']
    port = email_config['port']
    username = email_config['username']
    if username:
        password = email_config['password']
    else:
        password = None
    start_tls = email_config['start_tls']
    tls = email_config['tls']

    backend =  EmailBackend(host=server, port=port, username=username,
        password=password, use_tls=start_tls)
    return backend

def send_email(logger, email_config, html_part, start, project):
    from django.core.mail import send_mail
    text_part = strip_tags(html_part)
    subject = "Daily Telemetry Summary %s for %s" % (project, start)
    username = email_config['username']
    if username:
        password = email_config['password']
    else:
        password = None
    from_address = email_config['from_address']
    to_addresses = email_config['recipients'].split(',')
    num_retries = 0
    backend = get_email_backend(email_config)
    while num_retries < 5:
        try:
            logger.info('Sending email to %s...', ', '.join(to_addresses))
            send_mail(subject, text_part, from_address, to_addresses,
                      fail_silently=False, auth_user=username, auth_password=password, connection=backend, html_message=html_part)
            break
        except Exception, e:
            logger.error('fail to send email: %s', e)
            num_retries += 1
    else:
        logger.error('fail to send email after %d retries' % num_retries)
        return False
# main
def main():
    parser = argparse.ArgumentParser(description='Run T3 Log Report')
    parser.add_argument('--config', required=True, type=json_config, metavar = "config_file",
                   help='the config(json) file for the deployment', )
    parser.add_argument('--period',
                              help='Indicate the periodicity with which this job runs',
                              default=None, type=str)
    parser.add_argument('--start',
                              help='Time window starting time in ISO-8601 UTC',
                              action=DateTimeAction,
                              dest='start',
                              default=None)
    parser.add_argument('--end',
                              help='Time window ending time in ISO-8601 UTC or "now" for the current time',
                              action=DateTimeAction,
                              dest='end',
                              default=None)
    parser.add_argument('--email',
                              help='Send email notification',
                              action='store_true',
                              default=False)
    parser.add_argument('--logdir',
                              help='Where to write the logfiles. Default is ./logs/<config-file-basename>',
                              default=None, type=str)
    parser.add_argument('-d', '--debug',
                              help='Debug',
                              action='store_true',
                              default=False)
    args = parser.parse_args()
    config = args.config

    if not args.logdir:
        args.logdir = './log'
    if not os.path.exists(args.logdir):
        os.makedirs(args.logdir)
    log_file = os.path.abspath(os.path.join(args.logdir, 'event_report.log'))
    logging_format = '%(asctime)s.%(msecs)03d  %(levelname)-8s %(message)s'
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    handler = RFHandler(log_file, maxBytes=10*1024*1024, backupCount=10)
    handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    handler.setFormatter(logging.Formatter(logging_format))
    logger.addHandler(handler)

    streamhandler = logging.StreamHandler(sys.stdout)
    streamhandler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    streamhandler.setFormatter(logging.Formatter(logging_format))
    logger.addHandler(streamhandler)

    args.attachment_dir = os.path.join(args.logdir, "attachments")
    if not os.path.exists(args.attachment_dir):
        os.makedirs(args.attachment_dir)
    start, end = parse_dates(args)
    if not start:
        logger.error("Invalid start time(%s)/period(%s)", args.start, args.period)
        exit(-1)
    if not end:
        logger.error("Invalid end time(%s)/period(%s)", args.end, args.period)
        exit(-1)
    delete_logs(logger, config)
    upload_logs(logger, config, start, end)
    summary, error_list, warning_list = select_counts(logger, config, start, end)
    settings.configure(DEBUG=True, TEMPLATE_DEBUG=True, TEMPLATE_DIRS=('T3Viewer/templates',),
                       TEMPLATE_LOADERS=('django.template.loaders.filesystem.Loader',))
    report_data = {'summary': summary, 'errors': error_list, 'warnings': warning_list, "general_config": config["general_config"] }
    html_part = render_to_string('logreport.html', report_data)
    send_email(logger, config["email_config"], html_part, start, config['general_config']['project'])
    exit()

if __name__ == '__main__':
    main()
