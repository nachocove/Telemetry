from base import ObjectBase


class User(ObjectBase):
    PATH = 'users'

    def __init__(self, username=None, password=None, email=None, conn=None):
        ObjectBase.__init__(self, conn)
        self.connection = conn
        self.username = username
        self.password = password
        self.email = email
        self.session_token = None
        self.path = User.PATH

    def data(self):
        data = dict()
        data['username'] = self.username
        data['password'] = self.password
        if self.email:
            data['email'] = self.email
        if self.id is not None:
            data['objectId'] = self.id
        if self.session_token is not None:
            data['sessionToken'] = self.session_token
        return data

    def parse(self, data):
        if 'objectId' in data:
            self.id = data['objectId']
        if 'username' in data:
            self.username = data['username']
        if 'password' in data:
            self.password = data['password']
        if 'email' in data:
            self.email = data['email']
        if 'sessionToken' in data:
            self.session_token = data['sessionToken']