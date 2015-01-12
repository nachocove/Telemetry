#!/usr/bin/env python

import argparse
import ConfigParser
import logging
from datetime import timedelta, datetime, date
from boto.ec2 import cloudwatch

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
from monitors.monitor_count import MonitorUsers, MonitorEvents, MonitorEmails
from monitors.monitor_captures import MonitorCaptures
from monitors.monitor_counters import MonitorCounters
from monitors.monitor_hockeyapp import MonitorHockeyApp
from monitors.monitor_ui import MonitorUi
from monitors.monitor_support import MonitorSupport
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
        elif (option_string == '--before') and ('now' == value):
            setattr(namespace, self.dest, 'now')
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

def main():
    logging.basicConfig(format='%(asctime)s.%(msecs)03d  %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('monitor')
    logger.setLevel(logging.INFO)

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
               }


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

    config_group.add_argument('-d', '--debug',
                              help='Debug',
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


    misc_group = parser.add_argument_group(title='Miscellaneous Option')
    misc_group.add_argument('-h', '--help', help='Print this help message', action='store_true', dest='help')

    report_group = parser.add_argument_group('Monitors', 'These options select which report to run.')
    report_group.add_argument('monitors',
                              nargs='*',
                              metavar='MONITOR',
                              help='Choices are: %s' % ", ".join(mapping.keys()))
    options = parser.parse_args()

    if options.help:
        parser.print_help()
        exit(0)
    if options.weekly and options.daily:
        logger.error("Daily and weekly? Really? Pick one.")
        parser.print_help()
        exit(0)
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
    MonitorProfileConfig(config_file).read(options)
    if 'profile_name' in dir(options):
        logger.info('Running profile "%s"', options.profile_name)

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

    # If we want a time window but do not have one from command line, get it
    # from config and current time
    do_update_timestamp = False
    state_file = options.config + '.state'
    if isinstance(options.start, str) and options.start == 'last':
        try:
            timestamp_state = TimestampConfig(Config(state_file))
            options.start = timestamp_state.last
        except (Config.FileNotFoundException, ValueError) as e:
            logger.warn("Could not read last timestamp from file %s. Error=%s:%s.", state_file, e.__class__.__name__, e)
            if not options.period:
                raise ValueError("No period set. Can't guess 'last'. Please create state file manually.")
            try:
                options.start = guess_last(options.period)
            except ValueError as e:
                raise ValueError("Can't guess 'last': %s. Please create state file manually.", e)

            do_update_timestamp = True

    if isinstance(options.end, str) and options.end == 'now':
        options.end = UtcDateTime.now()
        do_update_timestamp = True
    if options.period:
        if not options.start:
            options.start = guess_last(options.period)
        options.end = UtcDateTime(options.start)
        options.end.datetime += timedelta(seconds=period_to_seconds(options.period))
        do_update_timestamp = True

    if options.debug:
        logger.setLevel(logging.DEBUG)

    # If send email, we want to make sure that the email credential is there
    summary_table = Summary()
    summary_table.colors = [None, '#f0f0f0']
    if options.email:
        (smtp_server, email) = EmailConfig(config_file).configure_server_and_email(recipients=options.email_to if options.email_to else None)
        if smtp_server is None:
            logger.error('no email configuration')
            exit(1)
    else:
        email = Email()
        smtp_server = None

    email.content = Html()
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
        if monitor_name not in mapping:
            logger.error('unknown monitor %s. ignore', monitor_name)
            continue
        extra_params = dict()
        if monitor_name == 'crashes':
            ha_obj = HockeyApp.hockeyapp.HockeyApp(options.hockeyapp_api_token)
            ha_app_obj = ha_obj.app(options.hockeyapp_app_id)
            extra_params['ha_app_obj'] = ha_app_obj
        elif monitor_name == 'cost':
            extra_params['cloudwatch'] = cloudwatch.connect_to_region('us-west-2',
                                                                      aws_secret_access_key=options.aws_secret_access_key,
                                                                      aws_access_key_id=options.aws_access_key_id,
                                                                      is_secure=True)

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
            new_monitor = monitor_cls(conn=conn, start=options.start, end=options.end, prefix=options.aws_prefix,
                                      **extra_params)
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
    with open('monitor-email.%s.html' % end_time_suffix, 'w') as f:
        f.write(email.content.html())
    with open('monitor-email.%s.txt' % end_time_suffix, 'w') as f:
        f.write(email.content.plain_text())

    # Add title
    email.subject = '%s [%s]' % (options.profile_name, str(options.end))

    if options.email and (summary_table.num_entries > 3):
        # Send the email
        num_retries = 0
        while num_retries < 5:
            try:
                logger.info('Sending email to %s...', ', '.join(email.to_addresses))
                email.send(smtp_server)
                break
            except Exception, e:
                logger.error('fail to send email: %s', e)
                num_retries += 1
        else:
            logger.error('fail to send email after %d retries' % num_retries)
    elif options.debug:
        print email.content.plain_text()

    # Update timestamp in config if necessary after we have successfully
    # send the notification email
    if do_update_timestamp:
        timestamp_state = TimestampConfig(Config(state_file, create=True))
        timestamp_state.last = options.end
        timestamp_state.save()

if __name__ == '__main__':
    main()
