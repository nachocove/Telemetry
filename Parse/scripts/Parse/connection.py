import json
import httplib
import socket
from exception import ParseException


class Connection:
    API_VERSION = '1'
    SERVER = 'api.parse.com'
    PORT = 443

    def __init__(self, app_id, api_key=None, session_token=None, master_key=None):
        self.app_id = app_id
        self.api_key = api_key
        self.session_token = session_token
        self.master_key = master_key
        self.connection = httplib.HTTPSConnection(Connection.SERVER, Connection.PORT)
        self.connection.connect()

    def header(self):
        hdr = dict()
        hdr['X-Parse-Application-Id'] = self.app_id
        if self.master_key is None:
            hdr['X-Parse-REST-API-KEY'] = self.api_key
        else:
            # If master key is provided, use that instead
            hdr['X-Parse-Master-Key'] = self.master_key
        if self.session_token is not None:
            hdr['X-Parse-Session-Token'] = self.session_token
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

        def convert_to_str(value):
            if isinstance(value, unicode):
                try:
                    return str(value)
                except UnicodeEncodeError:
                    return value
            if isinstance(value, dict):
                for key in value.keys():
                    if isinstance(value[key], unicode):
                        try:
                            value[key] = str(value[key])
                        except UnicodeEncodeError:
                            pass
                    if isinstance(key, unicode):
                        tmp_value = value[key]
                        del value[key]
                        value[str(key)] = tmp_value
            return value

        try:
            result = json.loads(html_result, object_hook=convert_to_str)
        except ValueError, e:
            print 'HTML response is:\n' + html_result
            raise ParseException(None, e.message)
        if 'error' in result:
            code = result.get('code', None)
            raise ParseException(code, result['error'])
        return result

    def post(self, path, data):
        return self.request('POST', path, json.dumps(data), self.post_header())

    def get(self, path):
        return self.request('GET', path, '', self.header())

    def put(self, path, data):
        return self.request('PUT', path, json.dumps(data), self.post_header())

    def delete(self, path):
        return self.request('DELETE', path, '', self.header())

    @staticmethod
    def create(app_id, api_key=None, session_token=None, master_key=None):
        num_retries = 0
        while num_retries < 10:
            try:
                return Connection(app_id, api_key, session_token, master_key)
            except socket.error, e:
                print 'WARN: fail to create connection (message=%s, num_retries=%d)' % (e.message, num_retries)
                num_retries += 1
        raise ParseException('fail to create connection after %d retries' % num_retries)
