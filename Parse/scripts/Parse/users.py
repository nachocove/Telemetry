from objects import Object
import urllib


class User(Object):
    PATH = 'users'

    def __init__(self, username=None, password=None, email=None, conn=None):
        Object.__init__(self, conn)
        self['username'] = username
        self['password'] = password
        self['email'] = email
        self.session_token = None

    def __str__(self):
        data = self.data()
        data['sessionToken'] = self.session_token
        return str(data)

    def path(self):
        return User.PATH

    def parse(self, data):
        Object.parse(self, data)
        self.session_token = self.pop('sessionToken', None)

    @staticmethod
    def login(username, password, conn):
        params = urllib.urlencode({'username': username, 'password': password})
        result = conn.get('login?%s' % params)
        user = User()
        user.parse(result)
        return user

    @staticmethod
    def query(conn, obj_id=None):
        return Object._query(User, User.PATH, conn, obj_id)