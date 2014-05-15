from objects import Object


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