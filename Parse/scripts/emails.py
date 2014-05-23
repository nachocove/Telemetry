import smtplib
import email.mime.text
import email.mime.multipart


class EmailServer:
    def __init__(self, server, port, username=None, password=None, tls=False, start_tls=False):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.start_tls = start_tls
        self.tls = tls

    def send(self, from_address, to_addresses, email_):
        if self.tls:
            smtp_svr = smtplib.SMTP_SSL()
        else:
            smtp_svr = smtplib.SMTP()
        smtp_svr.connect(self.server, self.port)
        if self.start_tls:
            smtp_svr.starttls()
        smtp_svr.login(self.username, self.password)
        smtp_svr.sendmail(from_address, to_addresses, email_)


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
            email_ = email.mime.text.MIMEText(self.content)
        else:
            email_ = email.mime.multipart.MIMEMultipart('alternative')
            plain_email = email.mime.text.MIMEText(self.content.plain_text(), 'plain')
            html_email = email.mime.text.MIMEText(self.content.html(), 'html')
            email_.attach(plain_email)
            email_.attach(html_email)
        email_['From'] = self.from_address
        email_['To'] = self.to_addresses_str()
        email_['Subject'] = self.subject

        server.send(self.from_address, self.to_addresses_str(), email_.as_string())
