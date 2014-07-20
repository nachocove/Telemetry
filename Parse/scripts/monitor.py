import argparse
import Parse
import HockeyApp
import config
import ConfigParser
import emails
import getpass
from html_elements import *
from monitor_base import Summary
from monitor_log import MonitorErrors, MonitorWarnings
from monitor_count import MonitorUsers, MonitorEvents
from monitor_captures import MonitorCaptures
from monitor_counters import MonitorCounters
from monitor_hockeyapp import MonitorHockeyApp


class MonitorConfig(config.Config):
    def __init__(self, cfg_file):
        config.Config.__init__(self, cfg_file)

    def read_timestamp(self):
        try:
            timestamp = self.config.get('timestamps', 'last')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            return None
        return Parse.utc_datetime.UtcDateTime(timestamp)

    def write_timestamp(self, utc_now):
        if not self.config.has_section('timestamps'):
            self.config.add_section('timestamps')
        self.config.set('timestamps', 'last', str(utc_now))
        self.write()

    def read_email_settings(self):
        email = emails.Email()
        try:
            server = self.config.get('email', 'smtp_server')
            port = self.config.getint('email', 'port')
            username = self.config.get('email', 'username')
            # We need to get the email account password
            if self.config.has_option('email', 'password'):
                # Option 1 - hardcoded into the file. highly not recommended.
                password = self.config.get('email', 'password')
            else:
                # Option 2 - try to get it from keychain
                try:
                    import keyring
                    password = keyring.get_password('NachoCove Telemetry', username)
                except ImportError:
                    password = None
                if password is None:
                    # Option 3 - user input
                    password = getpass.getpass('Email password: ')
                else:
                    print 'Got email account password from keychain.'
            if self.config.has_option('email', 'start_tls'):
                start_tls = self.config.getboolean('email', 'start_tls')
            else:
                start_tls = False
            if self.config.has_option('email', 'tls'):
                tls = self.config.getboolean('email', 'tls')
            else:
                tls = False

            email.from_address = username
            email.to_addresses = self.config.get('email', 'recipient').split(',')

            smtp_server = emails.EmailServer(server=server, port=port, username=username,
                                             password=password, tls=tls, start_tls=start_tls)
            return smtp_server, email
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            return None, None

    def read_hockeyapp_settings(self, options):
        self.get('keys', 'ha_app_id', options)
        self.get('keys', 'ha_api_token', options)


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
            setattr(namespace, self.dest, Parse.utc_datetime.UtcDateTime(value))


def datetime_tostr(iso_datetime):
    """
    This function returns a string from a UtcDateTime object that is good for
    usage as part of file paths.
    """
    datetime_str = str(iso_datetime)
    return datetime_str.replace(':', '_').replace('-', '_').replace('.', '_')


def main():
    parser = argparse.ArgumentParser(add_help=False)
    config_group = parser.add_argument_group(title='Configuration Options',
                                             description='These options specify the Parse credential and various '
                                                         'configurations. You need app id, REST API key, and session '
                                                         'token for user "monitor". To get the session token, use '
                                                         'parse.py login command. A configuration file is created to '
                                                         'store these parameters for you. So, you can omit these '
                                                         'parameters after running with them once.')
    config_group.add_argument('--api-key',
                              help='REST API key [default: NachoMail API key]',
                              default=None)
    config_group.add_argument('--app-id',
                              help='Application ID',
                              default=None)
    config_group.add_argument('--session-token',
                              help='Session token',
                              default=None)
    config_group.add_argument('--config',
                              help='Configuration file (default: monitor.cfg)',
                              default='monitor.cfg')
    config_group.add_argument('--email',
                              help='Send email notification',
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

    misc_group = parser.add_argument_group(title='Miscellaneous Option')
    misc_group.add_argument('-h', '--help', help='Print this help message', action='store_true', dest='help')

    report_group = parser.add_argument_group('Monitors', 'These options select which report to run.')
    report_group.add_argument('monitors',
                              nargs='*',
                              metavar='MONITOR',
                              help='Choices are: users, events, errors, warnings, captures, counters, crashes')
    options = parser.parse_args()

    if options.help or len(options.monitors) == 0:
        parser.print_help()
        exit(0)

    # If no key is provided in command line, get them from config.
    config_ = MonitorConfig(options.config)
    if not options.api_key or not options.app_id or not options.session_token:
        config_.read_keys(options)
    else:
        config_.write_keys(options)
    config_.read_hockeyapp_settings(options)

    # If we want a time window but do not have one from command line, get it
    # from config and current time
    do_update_timestamp = False
    if isinstance(options.start, str) and options.start == 'last':
        options.start = config_.read_timestamp()
    if isinstance(options.end, str) and options.end == 'now':
        options.end = Parse.utc_datetime.UtcDateTime.now()
        do_update_timestamp = True

    # If send email, we want to make sure that the email credential is there
    summary_table = Summary()
    summary_table.colors = [None, '#f0f0f0']
    if options.email:
        (smtp_server, email) = config_.read_email_settings()
        if smtp_server is None:
            print 'ERROR: no email configuration'
            exit(1)
        email.content = Html()
        email.content.add(Paragraph([Bold('Summary'), summary_table]))
    else:
        email = None
        smtp_server = None

    # Start processing
    print 'Start time: %s' % options.start
    print 'End time: %s' % options.end
    summary_table.add_entry('Start time', str(options.start))
    summary_table.add_entry('End time', str(options.end))

    # Run each monitor
    monitors = list()
    for monitor_name in options.monitors:
        mapping = {'errors': MonitorErrors,
                   'warnings': MonitorWarnings,
                   'users': MonitorUsers,
                   'events': MonitorEvents,
                   'captures': MonitorCaptures,
                   'counters': MonitorCounters,
                   'crashes': MonitorHockeyApp}
        if monitor_name not in mapping:
            print 'ERROR: unknown monitor %s. ignore' % monitor_name
            continue
        extra_params = list()
        if monitor_name == 'crashes':
            ha_obj = HockeyApp.hockeyapp.HockeyApp(options.ha_api_token)
            ha_app_obj = ha_obj.app(options.ha_app_id)
            extra_params.append(ha_app_obj)

        # Create a connection
        conn = Parse.connection.Connection.create(app_id=options.app_id,
                                                  api_key=options.api_key,
                                                  session_token=options.session_token)

        monitor = mapping[monitor_name](conn, options.start, options.end, *extra_params)
        monitors.append(monitor)
        monitor.run()

    # Generate all outputs
    for monitor in monitors:
        summary_table.toggle_color()
        output = monitor.report(summary_table)
        if options.email and output is not None:
            email.content.add(output)
        attachment_path = monitor.attachment()
        if attachment_path is not None and options.email:
            email.attachments.append(attachment_path)

    # Send the email
    if options.email:
        print 'Sending email...'
        # Save the HTML and plain text body to files
        end_time_suffix = datetime_tostr(options.end)
        with open('monitor-email.%s.html' % end_time_suffix, 'w') as f:
            print >>f, email.content.html()
        with open('monitor-email.%s.txt' % end_time_suffix, 'w') as f:
            print >>f, email.content.plain_text()
        # Add title
        email.subject = 'Telemetry summary [%s]' % str(options.end)
        email.send(smtp_server)

    # Update timestamp in config if necessary after we have successfully
    # send the notification email
    if do_update_timestamp:
        config_.write_timestamp(options.end)

if __name__ == '__main__':
    main()
