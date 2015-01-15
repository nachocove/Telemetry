import smtplib
import email.mime.base
import email.mime.text
import email.mime.multipart
from email import encoders
import os


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
        if self.username:
            smtp_svr.login(self.username, self.password)
        smtp_svr.sendmail(from_address, to_addresses, email_)
        smtp_svr.quit()


class Email:
    def __init__(self, debug=False):
        self.subject = ''
        self.content = None
        self.from_address = None
        self.to_addresses = []
        # A list of file paths of attachments
        self.attachments = []
        self.debug = debug

    def to_addresses_str(self):
        return ','.join(self.to_addresses)

    def send(self, server):
        if isinstance(self.content, str):
            # Text only email
            email_ = email.mime.text.MIMEText(self.content)
        else:
            # HTML email with plain text fallback
            email_ = email.mime.multipart.MIMEMultipart('alternative')
            plain_email = email.mime.text.MIMEText(self.content.plain_text(), 'plain')
            html_email = email.mime.text.MIMEText(self.content.html(), 'html')
            email_.attach(plain_email)
            email_.attach(html_email)

        if len(self.attachments) > 0:
            outer_email = email.mime.multipart.MIMEMultipart()
            outer_email.attach(email_)

            def encode_attachment(path):
                if self.debug:
                    print '  Attaching %s...' % path
                part = email.mime.base.MIMEBase('application', "octet-stream")
                part.set_payload(open(path, "rb").read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(path))
                return part

            for attachment in self.attachments:
                outer_email.attach(encode_attachment(attachment))
            email_ = outer_email

        email_['From'] = self.from_address
        email_['To'] = self.to_addresses_str()
        email_['Subject'] = self.subject

        if not self.debug:
            server.send(self.from_address, self.to_addresses_str(), email_.as_string())
        else:
            print "From: %(from)s\nTo: %(to)s\n\n%(email)s" %{'from': self.from_address, 'to': self.to_addresses_str(),
                                                              'email': email_.as_string()}
