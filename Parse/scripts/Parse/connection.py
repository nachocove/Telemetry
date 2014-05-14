import json
import httplib
from exception import ParseException


class Connection:
    API_VERSION = '1'
    SERVER = 'api.parse.com'
    PORT = 443

    def __init__(self, app_id, api_key):
        self.app_id = app_id
        self.api_key = api_key
        self.connection = httplib.HTTPSConnection(Connection.SERVER, Connection.PORT)
        self.connection.connect()

    def header(self):
        hdr = dict()
        hdr['X-Parse-Application-Id'] = self.app_id
        hdr['X-Parse-REST-API-KEY'] = self.api_key
        return hdr

    def post_header(self):
        hdr = self.header()
        hdr['Content-Type'] = 'application/json'
        return hdr

    def abspath(self, path):
        return '/' + Connection.API_VERSION + '/' + path

    def request(self, method, path, data, header):
        self.connection.request(method, self.abspath(path), data, header)
        html_result = self.connection.getresponse().read()
        result = json.loads(html_result)
        if 'error' in result:
            raise ParseException(result['code'], result['error'])
        return result

    def post(self, path, data):
        return self.request('POST', path, json.dumps(data), self.post_header())

    def get(self, path):
        return self.request('GET', path, '', self.header())
