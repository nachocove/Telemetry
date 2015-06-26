#!/usr/bin/env python

import argparse
import ConfigParser
import logging
from datetime import timedelta, datetime, date
import os
import copy
from boto.ec2 import cloudwatch
import sys
import shutil
from boto.s3.connection import S3Connection
from FreshDesk.config import FreshdeskConfig

try:
    from cloghandler import ConcurrentRotatingFileHandler as RFHandler
except ImportError:
    from logging.handlers import RotatingFileHandler as RFHandler

from AWS.config import AwsConfig
from AWS.tables import TelemetryTable
from AWS.connection import Connection
import HockeyApp
from HockeyApp.config import HockeyAppConfig
from misc import config
from misc.emails import Email
from misc.html_elements import *
from misc.utc_datetime import UtcDateTime
from misc.config import Config
from monitors.monitor_base import Summary, Monitor
from monitors.monitor_cost import MonitorCost
from monitors.monitor_log import MonitorErrors, MonitorWarnings
from monitors.monitor_count import MonitorUsers, MonitorEvents, MonitorEmails, MonitorUserDataUsage
from monitors.monitor_captures import MonitorCaptures
from monitors.monitor_counters import MonitorCounters
from monitors.monitor_hockeyapp import MonitorHockeyApp
from monitors.monitor_ui import MonitorUi
from monitors.monitor_support import MonitorSupport
from monitors.monitor_pinger import MonitorPingerPushMessages, MonitorPingerErrors, MonitorPingerWarnings, \
    MonitorClientPingerIssues
from monitors.config import EmailConfig, MonitorProfileConfig, TimestampConfig


class MonitorConfig(config.Config):
    def __init__(self, cfg_file):
        config.Config.__init__(self, cfg_file)

    def read_timestamp(self):
        try:
            timestamp = self.config.get('timestamps', 'last')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            return None
        return UtcDateTime(timestamp)

    def write_timestamp(self, utc_now):
        if not self.config.has_section('timestamps'):
            self.config.add_section('timestamps')
        self.config.set('timestamps', 'last', str(utc_now))
        self.write()


class DateTimeAction(argparse.Action):
    """
    This class parses the ISO-8601 UTC time given in --after and --before.
    """
    def __call__(self, parser, namespace, value, option_string=None):
        if (option_string != '--after') and (option_string != '--before'):
            raise ValueError('unexpected option %s with datetime ' % option_string)
        if (option_string == '--after') and ('last' == value):
            setattr(namespace, self.dest, 'last')
        elif (option_string == '--before') and (value.startswith('now')):
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

def today_midnight():
    return UtcDateTime(datetime.fromordinal(date.today().toordinal()))

def guess_last(period):
    ret = None
    if isinstance(period, (unicode, str)):
        if period == 'weekly':
            ret = last_sunday()
        elif period == 'daily':
            ret = today_midnight()
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

mapping = {'errors': MonitorErrors,
           'warnings': MonitorWarnings,
           'users': MonitorUsers,
           'emails': MonitorEmails,
           'events': MonitorEvents,
           'captures': MonitorCaptures,
           'counters': MonitorCounters,
           'crashes': MonitorHockeyApp,
           'ui': MonitorUi,
           'support': MonitorSupport,
           'cost': MonitorCost,
           'data-usage': MonitorUserDataUsage,
           'pinger-push': MonitorPingerPushMessages,
           'pinger-errors': MonitorPingerErrors,
           'pinger-warnings': MonitorPingerWarnings,
           'pinger-client': MonitorClientPingerIssues,
           }

def main():
    parser = argparse.ArgumentParser(add_help=False)
    config_group = parser.add_argument_group(title='Configuration Options',
                                             description='These options specify the Parse credential and various '
                                                         'configurations. You need app id, REST API key, and session '
                                                         'token for user "monitor". To get the session token, use '
                                                         'parse.py login command. A configuration file is created to '
                                                         'store these parameters for you. So, you can omit these '
                                                         'parameters after running with them once.')
    config_group.add_argument('--config',
                              help='Configuration file (default: monitor.cfg)',
                              default='monitor.cfg')
    config_group.add_argument('--email',
                              help='Send email notification',
                              action='store_true',
                              default=False)
    config_group.add_argument('--email-to',
                              help='Send email notification to a destination (override config file)',
                              action='append',
                              default=[])
    config_group.add_argument('--email-config',
                              help='Read email settings from a separate config file. Merge with existing file.',
                              type=str,
                              default=None)

    config_group.add_argument('-d', '--debug',
                              help='Debug',
                              action='store_true',
                              default=False)
    config_group.add_argument('-v', '--verbose',
                              help='Output logging to stdout',
                              action='store_true',
                              default=False)
    config_group.add_argument('--debug-boto',
                              help='Debug Boto',
                              action='store_true',
                              default=False)

    filter_group = parser.add_argument_group(title='Filtering Options',
                                             description='These options specify a time '
                                                         'window where reports are applied.')
    filter_group.add_argument('--after',
                              help='Time window starting time in ISO-8601 UTC or "last" for the last saved time',
                              action=DateTimeAction,
                              dest='start',
                              default=None)
    filter_group.add_argument('--before',
                              help='Time window ending time in ISO-8601 UTC or "now" for the current time',
                              action=DateTimeAction,
                              dest='end',
                              default=None)

    filter_group.add_argument('--period',
                              help='Indicate the periodicity with which this job runs',
                              default=None, type=str)
    filter_group.add_argument('--daily',
                              help='(DEPRECATED. Use --period daily). Set the ending time to exactly one day after the starting time',
                              action='store_true',
                              default=False)
    filter_group.add_argument('--weekly',
                              help='(DEPRECATED. Use --period weekly). Set the ending time to exactly one week after the starting time',
                              action='store_true',
                              default=False)

    filter_group.add_argument('--logdir',
                              help='Where to write the logfiles. Default is ./logs/<config-file-basename>',
                              default=None, type=str)

    misc_group = parser.add_argument_group(title='Miscellaneous Option')
    misc_group.add_argument('-h', '--help', help='Print this help message', action='store_true', dest='help')

    report_group = parser.add_argument_group('Monitors', 'These options select which report to run.')
    report_group.add_argument('monitors',
                              nargs='*',
                              metavar='MONITOR',
                              help='Choices are: %s' % ", ".join(mapping.keys()))
    options = parser.parse_args()

    logging_format = '%(asctime)s.%(msecs)03d  %(levelname)-8s %(message)s'
    logger = logging.getLogger('monitor')

    logger.setLevel(logging.INFO)
    if options.debug or options.verbose:
        streamhandler = logging.StreamHandler(sys.stdout)
        streamhandler.setLevel(logging.DEBUG if options.debug else logging.INFO)
        streamhandler.setFormatter(logging.Formatter(logging_format))
        logger.addHandler(streamhandler)

    if options.help:
        parser.print_help()
        exit(0)
    if options.weekly and options.daily:
        logger.error("Daily and weekly? Really? Pick one.")
        parser.print_help()
        exit(1)
    if options.weekly:
        logger.info('--weekly is DEPRECATED. Please use --period weekly')
        options.period = 'weekly'
    if options.daily:
        logger.info('--daily is DEPRECATED. Please use --period daily')
        options.period = 'daily'
    if options.email_to:
        options.email = True

    # If no key is provided in command line, get them from config.
    config_file = Config(options.config)
    AwsConfig(config_file).read(options)
    HockeyAppConfig(config_file).read(options)
    FreshdeskConfig(config_file).read(options)

    monitor_profile = MonitorProfileConfig(config_file)
    monitor_profile.read(options)

    if not options.debug and config_file.getbool('profile', 'debug'):
        options.debug = config_file.getbool('profile', 'debug')

    if not options.logdir and 'profile_logdir' in dir(options):
        options.logdir = options.profile_logdir
    if not options.logdir:
        options.logdir = './log'

    config_base, ext = os.path.splitext(os.path.basename(options.config))
    log_dir = os.path.join(options.logdir, config_base)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.abspath(os.path.join(log_dir, config_base + '.log'))
    options.attachment_dir = os.path.join(log_dir, "attachments")
    if not os.path.exists(options.attachment_dir):
        os.makedirs(options.attachment_dir)

    options.state_file = os.path.abspath(os.path.join(log_dir, config_base + '.state'))
    old_state_file_loc = options.config + '.state'

    handler = RFHandler(log_file, maxBytes=10*1024*1024, backupCount=10)
    handler.setLevel(logging.DEBUG if options.debug else logging.INFO)
    handler.setFormatter(logging.Formatter(logging_format))
    logger.addHandler(handler)

    if options.debug:
        logger.setLevel(logging.DEBUG)

    if os.path.exists(old_state_file_loc):
        logger.info("Moving old state file %s to %s", old_state_file_loc, options.state_file)
        shutil.move(old_state_file_loc, options.state_file)

    # Must have 1+ monitor from command-line or config
    if len(options.monitors) == 0:
        if 'profile_monitors' in dir(options) and len(options.profile_monitors) > 0:
            options.monitors = options.profile_monitors
        else:
            parser.print_help()
            exit(0)

    if 'aws_prefix' not in dir(options) or options.aws_prefix is None:
        logger.error('Missing "prefix" in "aws" section')
        exit(1)
    TelemetryTable.PREFIX = options.aws_prefix

    if options.email:
        emailConfig = EmailConfig(Config(options.email_config) if options.email_config else config_file)
        if emailConfig.recipient:
            logger.warn("Using 'recipient' in the email config is DEPRECATED. Please move it to the monitor profile section.")

        recipients = None
        if options.email_to:
            recipients = options.email_to
        elif monitor_profile.recipient:
            recipients = monitor_profile.recipient.split(',')
        elif emailConfig.recipient:
            recipients = emailConfig.recipient.split(',')
        if not recipients:
            logger.error('No email recipient list! No emails can be sent')
            return False
        else:
            email = emailConfig.configure_server_and_email(recipients=recipients)
            if email is None:
                logger.error('no email configuration')
                exit(1)
    else:
        email = Email()

    # If we want a time window but do not have one from command line, get it
    # from config and current time
    options.do_update_timestamp = False
    if isinstance(options.start, str) and options.start == 'last':
        options.do_update_timestamp = True
        try:
            timestamp_state = TimestampConfig(Config(options.state_file))
            options.start = timestamp_state.last
        except (Config.FileNotFoundException, ValueError) as e:
            logger.warn("Could not read last timestamp from file %s. Error=%s:%s.", options.state_file, e.__class__.__name__, e)
            if not options.period:
                raise ValueError("No period set. Can't guess 'last'. Please create state file manually.")
            try:
                options.start = guess_last(options.period)
            except ValueError as e:
                raise ValueError("Can't guess 'last': %s. Please create state file manually.", e)
    elif not options.start and options.period:
        options.start = guess_last(options.period)
        options.do_update_timestamp = True

    if not options.start:
        raise ValueError("No start time given or derived")

    period_end = None
    # TODO when calculating the end time, we might truncate to something close to the period, or the closest hour
    # so we're not going to times like 2015-04-16T08:10:26.447Z...
    orig_options_end = options.end if options.end else UtcDateTime(datetime.utcnow().replace(second=0, microsecond=0))
    if options.period:
        period_end = UtcDateTime(options.start.datetime + timedelta(seconds=period_to_seconds(options.period)))

    while options.start < orig_options_end:
        if period_end:
            options.end = period_end
            period_end = UtcDateTime(options.end.datetime + timedelta(seconds=period_to_seconds(options.period)))

        if not options.end or options.end > orig_options_end:
            options.end = orig_options_end

        ret = run_reports(copy.deepcopy(options), email, logger)
        if ret == False:
            break

        options.start = options.end


def run_reports(options, email, logger):
    assert isinstance(email, Email)

    logger.info("Monitor started: %s, cwd=%s, euid=%d", " ".join(sys.argv[1:]), os.getcwd(), os.geteuid())

    if 'profile_name' in dir(options):
        logger.info('Running profile "%s"', options.profile_name)

    # If send email, we want to make sure that the email credential is there
    summary_table = Summary()
    summary_table.colors = [None, '#f0f0f0']

    email.reset()
    email.content.add(Paragraph([Bold('Summary'), summary_table]))

    # Start processing
    logger.info('Start time: %s', options.start)
    logger.info('End time: %s', options.end)
    summary_table.add_entry('Start time', str(options.start))
    summary_table.add_entry('End time', str(options.end))
    summary_table.add_entry('Report period', str(options.end.datetime - options.start.datetime))

    # Run each monitor
    monitors = list()
    for monitor_name in options.monitors:
        kwargs = {'start': options.start,
                  'end': options.end,
                  'prefix': options.aws_prefix,
                  'attachment_dir': options.attachment_dir}

        if monitor_name in ['errors', 'warnings', 'users', 'events', 'crashes', 'support', 'pinger', 'emails']:
            try:
                if 'aws_isT3' in options and options.aws_isT3:
                    kwargs['isT3'] = options.aws_isT3
                    kwargs['s3conn'] = S3Connection(host='s3-us-west-2.amazonaws.com',
                                                   port=443,
                                                   aws_secret_access_key=options.aws_secret_access_key,
                                                   aws_access_key_id=options.aws_access_key_id,
                                                   is_secure=True)
                    kwargs['log_t3_bucket'] = options.aws_log_t3_bucket
                    kwargs['device_info_t3_bucket'] = options.aws_device_info_t3_bucket
                else:
                    kwargs['isT3'] = False
            except AttributeError:
                pass
        if monitor_name not in mapping:
            logger.error('unknown monitor %s. ignore', monitor_name)
            continue
        elif monitor_name == 'crashes':
            ha_obj = HockeyApp.hockeyapp.HockeyApp(options.hockeyapp_api_token)
            ha_app_obj = ha_obj.app(options.hockeyapp_app_id)
            kwargs['ha_app_obj'] = ha_app_obj
        elif monitor_name == 'emails':
            kwargs['support_t3_bucket'] = options.aws_support_t3_bucket
        elif monitor_name == 'support':
            try:
                freshdesk_options = {}
                for k in dir(options):
                    if k.startswith('freshdesk_'):
                        freshdesk_options[k.split('freshdesk_')[1]] = getattr(options, k)
                kwargs['freshdesk'] = freshdesk_options
                kwargs['bucket_name'] = options.aws_support_t3_bucket
            except AttributeError:
                pass
        elif monitor_name == 'cost':
            kwargs['cloudwatch'] = cloudwatch.connect_to_region('us-west-2',
                                                                      aws_secret_access_key=options.aws_secret_access_key,
                                                                      aws_access_key_id=options.aws_access_key_id,
                                                                      is_secure=True)
        elif monitor_name.startswith('pinger'):
            kwargs['bucket_name'] = options.aws_telemetry_bucket
            kwargs['path_prefix'] = options.aws_telemetry_prefix
            if monitor_name == 'pinger-push':
                kwargs['look_ahead'] = 180

        # Run the monitor with retries to robustly handle service failures
        def run_monitor():
            conn = Connection(host='dynamodb.us-west-2.amazonaws.com',
                              port=443,
                              aws_secret_access_key=options.aws_secret_access_key,
                              aws_access_key_id=options.aws_access_key_id,
                              region='us-west-2',
                              is_secure=True,
                              debug=2 if options.debug_boto else 0)
            monitor_cls = mapping[monitor_name]
            new_monitor = monitor_cls(conn=conn, **kwargs)
            new_monitor.run()
            return new_monitor
        monitor = Monitor.run_with_retries(run_monitor, 'monitor %s' % monitor_name, 5)
        monitors.append(monitor)

    # Generate all outputs
    for monitor in monitors:
        summary_table.toggle_color()
        output = monitor.report(summary_table, debug=options.debug)
        if email and output is not None:
            if isinstance(output, list):
                for element in output:
                    email.content.add(element)
            else:
                email.content.add(output)
        attachment_path = monitor.attachment()
        if attachment_path is not None and email:
            email.attachments.append(attachment_path)

    # Save the HTML and plain text body to files
    end_time_suffix = datetime_tostr(options.end)
    with open(os.path.join(options.attachment_dir, 'monitor-email.%s.html' % end_time_suffix), 'w') as f:
        f.write(email.content.html())
    with open(os.path.join(options.attachment_dir, 'monitor-email.%s.txt' % end_time_suffix), 'w') as f:
        f.write(email.content.plain_text())

    # Add title
    email.subject = '%s [%s]' % (options.profile_name, str(options.end))

    if options.email and (summary_table.num_entries > 3):
        # Send the email
        num_retries = 0
        while num_retries < 5:
            try:
                logger.info('Sending email to %s...', ', '.join(email.to_addresses))
                email.send()
                break
            except Exception, e:
                logger.error('fail to send email: %s', e)
                num_retries += 1
        else:
            logger.error('fail to send email after %d retries' % num_retries)
            return False
    elif options.debug:
        print email.content.plain_text()

    # Update timestamp in config if necessary after we have successfully
    # send the notification email
    if options.do_update_timestamp:
        timestamp_state = TimestampConfig(Config(options.state_file, create=True))
        timestamp_state.last = options.end
        timestamp_state.save()
    return True

if __name__ == '__main__':
    main()
