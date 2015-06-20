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
from AWS.s3t3_telemetry import T3_EVENT_CLASS_FILE_PREFIXES, get_T3_date_prefixes
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

def delete_all_logs(logger, config):
    aws_config = config["aws_config"]
    s3_config = config["s3_config"]
    try:
        logger.info("Creating connection...")
        conn = create_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Deleting logs...")
        sql_statement="delete from client_log"
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
def upload_logs(logger, config, event_class, start, end):
    aws_config = config["aws_config"]
    s3_config = config["s3_config"]
    upload_stats = {}
    try:
        logger.info("Creating connection...")
        conn = create_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Uploading logs...")
        event_classes = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
        if not isinstance(event_classes, list):
            event_classes = [event_class]
        for event_class in event_classes:
            event_class_stats = []
            upload_stats[event_class] = event_class_stats
            event_type= T3_EVENT_CLASS_FILE_PREFIXES[event_class]
            date_prefixes = get_T3_date_prefixes(start, end)
            for date_prefix in date_prefixes:
                sql_statement="COPY client_%s FROM 's3://%s/%s' \
                CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' \
                gzip maxerror 100000\
                json 's3://%s/%s'" % (event_type, s3_config[event_type]["t3_bucket"], date_prefix,
                                      aws_config["aws_access_key_id"], aws_config["aws_secret_access_key"],
                                      s3_config[event_type]["t3_bucket"], s3_config[event_type]["t3_jsonpath"])
                try:
                    logger.info(sql_statement)
                    cursor.execute(sql_statement)
                    logger.info("%s successful", cursor.statusmessage)# we dont get back a row count, no errors means we are good
                    conn.commit()
                    cursor.execute("select pg_last_copy_count();")
                    rows = cursor.fetchall()
                    rowsCopied = 0
                    for row in rows:
                        rowsCopied = row[0]
                        event_class_stats.append({"date":UtcDateTime(date_prefix).datetime.strftime('%Y-%m-%d'), "count":row[0]})
                    logger.info("Copied %s rows of %s for %s", rowsCopied, event_class, date_prefix)
                except Exception as err:
                    logger.error(err)
                conn.commit()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())
        cleanup(logger, config)
    return upload_stats

def get_upload_error_stats(logger, config, event_class, start, end):
    error_stats = {}
    return error_stats

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

def send_email(logger, email_config, html_part, start, project, attachments=None):
    from django.core.mail import send_mail
    text_part = strip_tags(html_part)
    subject = "Daily Redshift Upload Summary %s for %s" % (project, start)
    report_name = "RSUpload%s-%s" % (project, start)
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
            from django.core.mail import EmailMessage
            email = EmailMessage(subject, '', from_address,
                to_addresses, connection=backend)
            email.attach(report_name  + ".html", html_part, "text/html")
            email.attach(report_name + ".txt", text_part, "text/plain")
            import mimetypes
            for attachment in attachments:
                email.attach_file(attachment, mimetypes.guess_type(attachment)[0])
            email.send()
            #send_mail(subject, text_part, from_address, to_addresses,
            #         fail_silently=False, auth_user=username, auth_password=password, connection=backend, html_message=html_part)
            break
        except Exception, e:
            logger.error('fail to send email: %s', e)
            logger.error(traceback.format_exc())
            num_retries += 1
    else:
        logger.error('fail to send email after %d retries' % num_retries)
        return False
# main
def main():
    parser = argparse.ArgumentParser(description='T3 RedShift Loader')
    parser.add_argument('--config', required=True, type=json_config, metavar = "config_file",
                   help='the config(json) file for the deployment', )
    parser.add_argument('--period',
                              help='Indicate the periodicity with which this job runs',
                              default=None, type=str)
    parser.add_argument('--start',
                              help='Date window starting time in ISO-8601 UTC. e.g 2015-06-18',
                              action=DateTimeAction,
                              dest='start',
                              default=None)
    parser.add_argument('--end',
                              help='Date window ending time in ISO-8601 UTC or "now" for the current time. e.g 2015-06-18',
                              action=DateTimeAction,
                              dest='end',
                              default=None)
    parser.add_argument('--event_type',
                              help="Event Type. Specify one of 'PROTOCOL','LOG', 'COUNTER', \
                                   'STATISTICS2','UI', 'DEVICEINFO', 'SAMPLES' if you don't need all",
                              default='REDSHIFT',
                              type=str)
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
    start, end = parse_dates(args)

    if not args.logdir:
        args.logdir = './log'
    if not os.path.exists(args.logdir):
        os.makedirs(args.logdir)
    log_filename = 't3_redshift_loader%s-%s.log' % (start.datetime.strftime('%Y%m%d'), end.datetime.strftime('%Y%m%d'))
    log_file = os.path.abspath(os.path.join(args.logdir, log_filename))
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

    if args.period and args.period != 'daily':
        logger.error("Invalid period (%s). Only daily is supported for now.", args.period)
        exit(-1)
    if args.period and args.start and args.end:
        logger.warn("Ignoring period (%s). Both start (%s) and end (%s) are defined.", args.period, start, end)
        exit(-1)
    if not start:
        logger.error("Invalid start time(%s)/period(%s)", args.start, args.period)
        exit(-1)
    if not end:
        logger.error("Invalid end time(%s)/period(%s)", args.end, args.period)
        exit(-1)
    if (args.event_type not in T3_EVENT_CLASS_FILE_PREFIXES):
        logger.error("Invalid event type %s. Pick one of %s", args.event_type, T3_EVENT_CLASS_FILE_PREFIXES.keys())
        exit(-1)
    if args.event_type == "ALL":
        args.event_type = "REDSHIFT" # limit the redshiftable logs
    summary = {}
    summary["start"] = start
    summary["end"] = end
    event_types = T3_EVENT_CLASS_FILE_PREFIXES[args.event_type]
    if isinstance(event_types, list):
        summary["event_types"] = event_types
    else:
        summary["event_types"] = args.event_type
    upload_stats = {}
    upload_stats["log"] = [{"date": "2", "count":22}, {"date": "3", "count":44}]
    #upload_stats = upload_logs(logger, config, args.event_type, start, end)
    error_stats = get_upload_error_stats(logger, config, args.event_type, start, end)
    settings.configure(DEBUG=True, TEMPLATE_DEBUG=True, TEMPLATE_DIRS=('T3Viewer/templates',),
                       TEMPLATE_LOADERS=('django.template.loaders.filesystem.Loader',))
    report_data = {'summary': summary, 'upload_stats': upload_stats, "general_config": config["general_config"]}
    html_part = render_to_string('uploadreport.html', report_data)
    if args.email:
        send_email(logger, config["email_config"], html_part, start, config['general_config']['project'], [os.path.join(args.logdir, 'event_report.log')])
    elif args.debug:
        print html_part
    exit()

if __name__ == '__main__':
    main()
