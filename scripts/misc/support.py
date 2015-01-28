import json
import hashlib


class SupportEvent:
    def __init__(self, event):
        self.client = event['client']
        self.timestamp = str(event['timestamp'])
        self.params = json.loads(event['support'])
        if self.params is None:
            # There are some bad SUPPORT events in telemetry. They were
            # created during development / testing of the API.
            self.params = dict()

    def display(self):
        msg = ''
        msg += 'timestamp; %s\n' % self.timestamp
        msg += 'client: %s\n' % self.client
        return msg


class SupportRequestEvent(SupportEvent):
    def __init__(self, event):
        SupportEvent.__init__(self, event)
        self.contact_info = self.params['ContactInfo']
        self.message = self.params['Message']

    def display(self):
        msg = SupportEvent.display(self)
        msg += 'contact_info: %s\n' % self.contact_info
        msg += 'message: %s\n' % self.message
        return msg

    @staticmethod
    def parse(event):
        try:
            return SupportRequestEvent(event)
        except KeyError:
            return None


class SupportSha256EmailAddressEvent(SupportEvent):
    def __init__(self, event):
        SupportEvent.__init__(self, event)
        self.sha256_email_address = self.params['sha256_email_address']

    def display(self):
        msg = SupportEvent.display(self)
        msg += 'sha256_email_address: %s\n' % self.sha256_email_address
        return msg

    @staticmethod
    def parse(event):
        try:
            return SupportSha256EmailAddressEvent(event)
        except KeyError:
            return None


class Support:
    def __init__(self):
        pass  # doesn't do anything but keep PyCharm from complaining

    @staticmethod
    def filter(events, classes):
        event_objects = []
        for event in events:
            for class_ in classes:
                obj = class_.parse(event)
                if obj is not None:
                    event_objects.append(obj)
                    break
        return event_objects

    @staticmethod
    def get_support_requests(events):
        return Support.filter(events, [SupportRequestEvent])

    @staticmethod
    def get_sha256_email_address(events, email_address):
        index = email_address.find('@')
        if 0 > index:
            raise ValueError('Invalid email address')
        if index != email_address.rfind('@'):
            raise ValueError('Invalid email address')
        email, domain = email_address.split('@')

        obfuscated = "%s@%s" % (hashlib.sha256(email).hexdigest(), domain)
        email_events = Support.filter(events, [SupportSha256EmailAddressEvent])
        filtered_events = filter(lambda x: x.sha256_email_address == obfuscated, email_events)
        if len(filtered_events) == 0 and len(email) == 64:
            obfuscated = email_address
            # perhaps the email given is already an obfuscated one? Let's try it.
            filtered_events = filter(lambda x: x.sha256_email_address == obfuscated, email_events)
        return obfuscated, filtered_events

