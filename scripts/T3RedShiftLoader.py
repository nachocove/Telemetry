import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime

from django.conf import settings
from django.template.loader import render_to_string
from django.utils.html import strip_tags

from AWS.db_reports import parse_dates
from AWS.redshift_handler import upload_logs, create_tables, delete_logs
from AWS.s3_telemetry import create_s3_conn
from AWS.s3t3_telemetry import T3_EVENT_CLASS_FILE_PREFIXES
from misc.utc_datetime import UtcDateTime

try:
    from cloghandler import ConcurrentRotatingFileHandler as RFHandler
except ImportError:
    from logging.handlers import RotatingFileHandler as RFHandler


# load json config
def json_config(file_name):
    with open(file_name) as data_file:
        json_data = json.load(data_file)
    return json_data


def get_user_device_prefixes(logger, config, startdt_prefix):
    aws_config = config["aws_config"]
    s3_config = config["s3_config"]
    prefixes = []
    conn = create_s3_conn(aws_config["aws_access_key_id"], aws_config["aws_secret_access_key"])
    bucket_name = s3_config["client_t3_log_bucket"]
    bucket = conn.get_bucket(bucket_name)
    for l1_prefix in bucket.list(prefix=startdt_prefix + '/', delimiter='/'):
        for l2_prefix in bucket.list(prefix=l1_prefix.name, delimiter='/'):
            for l3_prefix in bucket.list(prefix=l2_prefix.name, delimiter='/'):
                prefixes.append(l3_prefix.name + 'NachoMail')
    return prefixes


def get_upload_error_stats(logger, config, event_class, start, end):
    error_stats = {}
    return error_stats


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

    backend = EmailBackend(host=server, port=port, username=username,
                           password=password, use_tls=start_tls)
    return backend


def send_email(logger, email_config, html_part, start, project_name, attachments=None):
    text_part = strip_tags(html_part)
    subject = "Daily Redshift Upload Summary %s for %s" % (project_name, start)
    report_name = "RSUpload%s-%s" % (project_name, start)
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
            email.attach(report_name + ".html", html_part, "text/html")
            email.attach(report_name + ".txt", text_part, "text/plain")
            import mimetypes
            for attachment in attachments:
                email.attach_file(attachment, mimetypes.guess_type(attachment)[0])
            email.send()
            # send_mail(subject, text_part, from_address, to_addresses,
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
    parser.add_argument('--config', required=True, type=json_config, metavar="config_file",
                        help='the config(json) file for the deployment', )
    parser.add_argument('--period',
                        help='Indicate the periodicity with which this job runs',
                        default=None, type=str)
    parser.add_argument('--start',
                        help='Date window starting time in ISO-8601 UTC. e.g 2015-06-18',
                        dest='start',
                        default=None)
    parser.add_argument('--end',
                        help='Date window ending time in ISO-8601 UTC or "now" for the current time. e.g 2015-06-18',
                        dest='end',
                        default=None)
    parser.add_argument('--event_class',
                        help="Event Class. Specify one of 'PROTOCOL','LOG', 'COUNTER', \
                                   'STATISTICS2','UI', 'DEVICEINFO', 'SAMPLES', 'TIMESERIES',\
                                    'SUPPORT', 'PINGER', if you don't need all",
                        default='ALL',
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
    parser.add_argument('--no-delete',
                        help="Don't delete timespan before loading.",
                        default=False,
                        action="store_true")
    parser.add_argument('--prefix',
                        help="The table prefix",
                        default=None,
                        type=str)

    args = parser.parse_args()
    config = args.config
    start, end = parse_dates(args)

    project = config['general_config']['project']

    if not args.logdir:
        args.logdir = './log'
    if not os.path.exists(args.logdir):
        os.makedirs(args.logdir)
    log_filename = 't3_redshift_loader-%s-%s-%s.%s.log' % (
    project, start.datetime.strftime('%Y%m%d'), end.datetime.strftime('%Y%m%d'), UtcDateTime(datetime.now()))
    log_file = os.path.abspath(os.path.join(args.logdir, log_filename))
    logging_format = '%(asctime)s.%(msecs)03d  %(levelname)-8s %(message)s'
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    handler = RFHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=10)
    handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    handler.setFormatter(logging.Formatter(logging_format))
    logger.addHandler(handler)

    if args.debug:
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
    if args.event_class not in T3_EVENT_CLASS_FILE_PREFIXES.keys():
        logger.error("Invalid event class %s. Pick one of %s", args.event_class, T3_EVENT_CLASS_FILE_PREFIXES.keys())
        exit(-1)
    summary = {}
    summary["start"] = start
    summary["end"] = end
    event_classes = T3_EVENT_CLASS_FILE_PREFIXES[args.event_class]
    if isinstance(event_classes, list):
        summary["event_classes"] = event_classes
        for ev_class in event_classes:
            if "table_name" in summary:
                summary["table_name"] = summary["table_name"] + ", " + \
                                        project + \
                                        "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[ev_class]
            else:
                summary["table_name"] = project + \
                                        "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[ev_class]
    else:
        summary["event_classes"] = args.event_class
        summary["table_name"] = "nm_" + T3_EVENT_CLASS_FILE_PREFIXES[args.event_class]

    logger.info("Running T3 Redshift Uploader for the period %s to %s", start, end)

    create_tables(logger, project, config, args.event_class, args.prefix)
    if not args.no_delete:
        delete_logs(logger, project, config, args.event_class, start, end, args.prefix)
    upload_stats = upload_logs(logger, project, config, args.event_class, start, end, args.prefix)
    get_upload_error_stats(logger, config, args.event_class, start, end)
    template_dir = config['general_config']['src_root'] + '/T3Viewer/templates'
    settings.configure(DEBUG=True, TEMPLATE_DEBUG=True, TEMPLATE_DIRS=(template_dir,),
                       TEMPLATE_LOADERS=('django.template.loaders.filesystem.Loader',))
    report_data = {'summary': summary, 'upload_stats': upload_stats, "general_config": config["general_config"]}
    html_part = render_to_string('upload_report_plain.html', report_data)
    if args.email:
        send_email(logger, config["email_config"], html_part, start,
                   project, [os.path.join(args.logdir, log_filename)])
    elif args.debug:
        print html_part
    exit()


if __name__ == '__main__':
    main()
