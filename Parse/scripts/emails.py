import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class EmailServer:
    def __init__(self, server, port, username=None, password=None, tls=False, start_tls=False):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.start_tls = start_tls
        self.tls = tls

    def send(self, from_address, to_addresses, email):
        if self.tls:
            smtp_svr = smtplib.SMTP_SSL()
        else:
            smtp_svr = smtplib.SMTP()
        smtp_svr.connect(self.server, self.port)
        if self.start_tls:
            smtp_svr.starttls()
        smtp_svr.login(self.username, self.password)
        smtp_svr.sendmail(from_address, to_addresses, email)


class Email:
    def __init__(self):
        self.subject = ''
        self.content = []
        self.from_address = None
        self.to_addresses = []

    def to_addresses_str(self):
        return ','.join(self.to_addresses)

    def send(self, server):
        if isinstance(self.content, str):
            email = MIMEText(self.content)
        else:
            email = MIMEMultipart('alternative')
            plain_email = MIMEText(self.content.plain_text(), 'plain')
            html_email = MIMEText(self.content.html(), 'html')
            email.attach(plain_email)
            email.attach(html_email)
        email['From'] = self.from_address
        email['To'] = self.to_addresses_str()
        email['Subject'] = self.subject

        server.send(self.from_address, self.to_addresses_str(), email.as_string())
