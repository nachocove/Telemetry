# Copyright 2014, NachoCove, Inc


# This script requires "requests": http://docs.python-requests.org/
# To install: pip install requests

import json
import requests


class FreshDesk(object):
    FRESHDESK_DOMAIN = "nachocove.freshdesk.com"

    headers = {'Content-Type': 'application/json'}

    STATUS_OPEN = 1

    def __init__(self, api_key, use_https=True, hostname=None):
        self.hostname = hostname or self.FRESHDESK_DOMAIN
        self.api_key = api_key
        self.api_url = "%s://%s" % ("https" if use_https else "http", self.hostname)

    def _send_request(self, path, payload):
        try:
            r = requests.post(self.api_url + path,
                              auth=(self.api_key, "X"),
                              headers=self.headers,
                              data=json.dumps(payload),
                              allow_redirects=False)
            if r.status_code != 200:
                raise self.FreshDeskException(r.status_code)
            return json.loads(r.content)
        except requests.RequestException as e:
            raise self.FreshDeskException(e)

    class FreshDeskException(Exception):
        pass

    def create_ticket(self, subject, description, email, priority=1, status=STATUS_OPEN, cc_emails=None):
        """

        :param subject: the ticket subject
        :param description: the ticket description
        :param email: the user's email
        :param priority: the priority
        :param status: the status
        :return: the ticket ID
        """
        if cc_emails is None:
            cc_emails = ()
        assert isinstance(cc_emails, (list, tuple))
        path = '/helpdesk/tickets.json'
        payload = {
            'helpdesk_ticket': {
                'subject': subject,
                'description': description,
                'email': email,
                'priority': priority,
                'status': status
            },
            'cc_emails': ",".join(cc_emails)
        }
        response = self._send_request(path, payload)
        assert 'helpdesk_ticket' in response and 'display_id' in response['helpdesk_ticket']
        return response['helpdesk_ticket']['display_id']

    def add_note(self, ticket_id, note, private=True):
        path = "/helpdesk/tickets/%s/conversations/note.json" % ticket_id
        payload = {"helpdesk_note": {"body_html": note, "private": private}}
        response = self._send_request(path, payload)
        assert 'note' in response and 'id' in response['note']
        return response['note']['id']
