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
    def __init__(self, email_server=None, debug=False):
        self.debug = debug
        self.email_server = email_server
        self.from_address = None
        self.to_addresses = []
        self.reset()

    def reset(self):
        from misc.html_elements import Html
        self.subject = ''
        self.content = Html()
        # A list of file paths of attachments
        self.attachments = []

    def to_addresses_str(self):
        return ','.join(self.to_addresses)

    def send(self):
        if isinstance(self.content, str):
            # Text only email
            email_ = email.mime.text.MIMEText(self.content)
        else:
            # HTML email with plain text fallback
            email_ = email.mime.multipart.MIMEMultipart('alternative')
            plain_email = email.mime.text.MIMEText(self.content.plain_text(), 'plain', _charset='utf-8')
            html_email = email.mime.text.MIMEText(self.content.html(), 'html', _charset='utf-8')
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

        if not self.debug and self.email_server:
            self.email_server.send(self.from_address, self.to_addresses, email_.as_string())
        else:
            print "From: %(from)s\nTo: %(to)s\n\n%(email)s" %{'from': self.from_address, 'to': self.to_addresses_str(),
                                                              'email': email_.as_string()}

def emails_per_domain(email_addresses):
    emails_per_domain_dict = dict()
    for email in email_addresses:
        userhash, domain = email.split('@')
        if domain not in emails_per_domain_dict:
            emails_per_domain_dict[domain] = []
        emails_per_domain_dict[domain].append(email)
    return emails_per_domain_dict
