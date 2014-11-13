import getpass
import logging
import keyring
from misc.config import SectionConfig
from misc.emails import Email, EmailServer


class MonitorProfileConfig(SectionConfig):
    SECTION = 'profile'
    KEYS = (
        'monitors',
        'name'
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)

    def read(self, options):
        SectionConfig.read(self, options)
        if 'profile_monitors' in dir(options):
            options.monitors = options.profile_monitors.split(',')


class EmailConfig(SectionConfig):
    SECTION = "email"
    KEYS = (
        'smtp_server',
        'port',
        'start_tls',
        'tls',
        'username',
        'password',
        'recipient'
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)

    def __getattr__(self, key):
        if key == 'port':
            return self.config_file.getint(EmailConfig.SECTION, key)
        elif key == 'tls' or key == 'start_tls':
            return self.config_file.getbool(EmailConfig.SECTION, key)
        return SectionConfig.__getattr__(self, key)

    def configure_server_and_email(self):
        email = Email()
        server = self.smtp_server
        port = self.port
        username = self.username
        # We need to get the email account password
        if self.password is not None:
            # Option 1 - hardcoded into the file. highly not recommended.
            password = self.password
        else:
            # Option 2 - try to get it from keychain
            try:
                password = keyring.get_password('NachoCove Telemetry', username)
            except ImportError:
                password = None
            if password is None:
                # Option 3 - user input
                password = getpass.getpass('Email password: ')
            else:
                logging.getLogger('monitor').info('Got email account password from keychain.')

        start_tls = False
        if self.start_tls is not None:
            start_tls = self.start_tls

        tls = False
        if self.tls is not None:
            tls = self.tls

        email.from_address = username
        email.to_addresses = self.recipient.split(',')

        smtp_server = EmailServer(server=server,
                                  port=port,
                                  username=username,
                                  password=password,
                                  tls=tls,
                                  start_tls=start_tls)
        return smtp_server, email


class TimestampConfig(SectionConfig):
    SECTION = 'timestamp'
    KEYS = (
        'last'
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)

    def save(self):
        self.config_file.write()
