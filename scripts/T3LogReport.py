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
from AWS.db_reports import log_report, parse_dates, classify_log_events

try:
    from cloghandler import ConcurrentRotatingFileHandler as RFHandler
except ImportError:
    from logging.handlers import RotatingFileHandler as RFHandler

# load json config
def json_config(file_name):
    with open(file_name) as data_file:
        json_data = json.load(data_file)
    #pprint(json_data)
    return json_data

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

def send_email(logger, email_config, html_part, start, project_name):
    from django.core.mail import send_mail
    text_part = strip_tags(html_part)
    subject = "Daily Telemetry Summary %s for %s" % (project_name, start)
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
                              dest='start',
                              default=None)
    parser.add_argument('--end',
                              help='Time window ending time in ISO-8601 UTC or "now" for the current time',
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

    if args.debug:
        streamhandler = logging.StreamHandler(sys.stdout)
        streamhandler.setLevel(logging.DEBUG if args.debug else logging.INFO)
        streamhandler.setFormatter(logging.Formatter(logging_format))
        logger.addHandler(streamhandler)

    args.attachment_dir = os.path.join(args.logdir, "attachments")
    if not os.path.exists(args.attachment_dir):
        os.makedirs(args.attachment_dir)
    if not args.start and not args.end and not args.period:
        logger.error("You must specify start or end or period for the report")
        exit(-1)
    start, end = parse_dates(args)
    if not start:
        logger.error("Invalid start time(%s)/period(%s)", args.start, args.period)
        exit(-1)
    if not end:
        logger.error("Invalid end time(%s)/period(%s)", args.end, args.period)
        exit(-1)
    logger.info("Running log report for the period %s to %s", start, end)
    summary, error_list, warning_list = log_report(logger, config['general_config']['project'], config, start, end)
    logger.info("Classifying log events")
    clustered_error_list=classify_log_events(error_list)
    logger.info("Clustered errors count %s", len(clustered_error_list))
    clustered_warning_list=classify_log_events(warning_list)
    logger.info("Clustered warning count %s", len(clustered_warning_list))
    logger.info("Creating html report")
    template_dir = config['general_config']['src_root'] + '/T3Viewer/templates'
    settings.configure(DEBUG=True, TEMPLATE_DEBUG=True, TEMPLATE_DIRS=(template_dir,),
                       TEMPLATE_LOADERS=('django.template.loaders.filesystem.Loader',))
    report_data = {'summary': summary, 'errors': error_list[:1000], 'warnings': warning_list[:1000],
                   'clustered_errors': clustered_error_list,
                   'clustered_warnings': clustered_warning_list,
                   "general_config": config["general_config"]}
    html_part = render_to_string('log_report_plain.html', report_data)
    if args.email:
        send_email(logger, config["email_config"], html_part, start, config['general_config']['project_name'])
    elif args.debug:
        print html_part
    exit()

if __name__ == '__main__':
    main()
