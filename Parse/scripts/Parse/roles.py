from objects import Object
from users import User
from relation import Relation


class Role(Object):
    PATH = 'roles'

    def __init__(self, name=None, conn=None):
        Object.__init__(self, conn)
        self['name'] = name
        self.roles = Relation(Role)
        self.users = Relation(User)
        self.path = Role.PATH

    def path(self):
        return Role.PATH

    def data(self, create=True, addition=True):
        data = self.data()
        roles_data = self.roles.data(create, addition)
        if len(roles_data) > 0:
            data['roles'] = roles_data
        users_data = self.users.data(create, addition)
        if len(users_data) > 0:
            data['users'] = users_data
        return data

    def parse(self, data):
        Object.parse(self, data)

    def create(self, conn=None):
        conn = self._check_conn(conn)
        result = conn.post(self.path, self.data(create=True, addition=True))
        self.parse(result)

    @staticmethod
    def query(cls, conn, obj_id=None):
        return Object._query(Role, Role.PATH, conn, obj_id)

