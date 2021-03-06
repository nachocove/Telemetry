import json
import hashlib

def obfuscate_email(email_address):
    index = email_address.find('@')
    if 0 > index:
        raise ValueError('Invalid email address')
    if index != email_address.rfind('@'):
        raise ValueError('Invalid email address')
    email, domain = email_address.split('@')

    return "%s@%s" % (hashlib.sha256(email.lower()).hexdigest(), domain)

class SupportEvent:
    def __init__(self, event):
        self.client = event['client']
        self.timestamp = str(event['timestamp'])
        self.params = json.loads(event['support'])
        if 'key_name' in event:
            self.key_name = event['key_name']
        else:
            self.key_name = ''
        if self.params is None:
            # There are some bad SUPPORT events in telemetry. They were
            # created during development / testing of the API.
            self.params = dict()

    def display(self):
        msg = ''
        msg += 'timestamp; %s\n' % self.timestamp
        msg += 'client: %s\n' % self.client
        return msg

    def __str__(self):
        return self.display()

class SupportRequestEvent(SupportEvent):
    def __init__(self, event):
        SupportEvent.__init__(self, event)
        self.contact_info = self.params['ContactInfo']
        self.message = self.params['Message']
        self.build_version = self.params.get('BuildVersion', '')
        self.build_number = self.params.get('BuildNumber', '')

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


class SupportBackLogEvent(SupportEvent):
    def __init__(self, event):
        SupportEvent.__init__(self, event)
        self.num_events = self.params['num_events']
        self.oldest_event = self.params['oldest_event']

    def display(self):
        msg = SupportEvent.display(self)
        msg += 'num_events: %s\noldest_event: %s\n' % (self.num_events, self.oldest_event)
        return msg

    @staticmethod
    def parse(event):
        try:
            return SupportBackLogEvent(event)
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
    def get_email_address_clients(events, email_address):
        email, domain = email_address.split('@')
        email_events = Support.filter(events, [SupportSha256EmailAddressEvent])
        if email:
            obfuscated = obfuscate_email(email_address)
            filtered_events = filter(lambda x: x.sha256_email_address == obfuscated, email_events)
            if len(filtered_events) == 0 and len(email) == 64:
                obfuscated = email_address
                # perhaps the email given is already an obfuscated one? Let's try it.
                filtered_events = filter(lambda x: x.sha256_email_address == obfuscated, email_events)
        elif domain:
            filtered_events = filter(lambda x: x.sha256_email_address.endswith('@'+domain), email_events)
            obfuscated = None
        else:
            raise Exception("Bad email %s", email_address)
        return obfuscated, filtered_events

