from base import ObjectBase
from acl import Acl
from users import User
from relation import Relation


class Role(ObjectBase):
    PATH = 'roles'

    @staticmethod
    def query(conn, obj_id=None):
        path = Role.PATH
        if obj_id is not None:
            path += '/' + obj_id
        result = conn.get(path)

        # Create Role objects
        if obj_id is None:
            roles = []
            assert 'results' in result
            for role in result['results']:
                new_obj = Role()
                new_obj.parse(role)
                roles.append(new_obj)
        else:
            roles = Role()
            roles.parse(result)
        return roles

    def __init__(self, name=None, id=None, conn=None):
        ObjectBase.__init__(self, conn)
        self.name = name
        self.acl = None
        self.roles = Relation(Role)
        self.users = Relation(User)
        self.path = Role.PATH

    def data(self, create=True, addition=True):
        data = dict()
        data['name'] = self.name
        if self.acl is not None:
            data['ACL'] = self.acl.data()
        roles_data = self.roles.data(create, addition)
        if len(roles_data) > 0:
            data['roles'] = roles_data
        users_data = self.users.data(create, addition)
        if len(users_data) > 0:
            data['users'] = users_data
        return data

    def parse(self, data):
        if 'name' in data:
            self.name = data['name']
        if 'ACL' in data:
            self.acl = Acl()
            self.acl.parse(data['ACL'])

    def create(self, conn=None):
        conn = self._check_conn(conn)
        result = conn.post(self.path, self.data(create=True, addition=True))
        self.parse(result)