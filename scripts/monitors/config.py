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

    def read(self, options):
        SectionConfig.read(self, options)
        if 'recipient' in dir(options):
            options.recipient = options.recipient.split(',')


class EmailConfig(SectionConfig):
    SECTION = "email"
    KEYS = (
        'smtp_server',
        'port',
        'start_tls',
        'username',
        'recipient'
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)

    def configure_server_and_email(self):
        email = Email()
        email_config = EmailConfig(self)
        server = email_config.server
        port = email_config.port
        username = email_config.username
        # We need to get the email account password
        if email_config.password is not None:
            # Option 1 - hardcoded into the file. highly not recommended.
            password = email_config.password
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
        if email_config.start_tls is None:
            start_tls = email_config.start_tls

        tls = False
        if email_config.tls is not None:
            tls = email_config.tls

        email.from_address = username
        email.to_addresses = email_config.recipient.split(',')

        smtp_server = EmailServer(server=server,
                                  port=port,
                                  username=username,
                                  password=password,
                                  tls=tls,
                                  start_tls=start_tls)
        return smtp_server, email
