from acl import Acl
from exception import ParseException


class Object(dict):
    def __init__(self, conn=None, class_name=None):
        dict.__init__(self)
        self.connection = conn
        self.id = None
        self.acl = None
        self.class_name = class_name

    def __str__(self):
        data = self.data()
        data['objectId'] = self.id
        return str(data)

    def _check_conn(self, conn):
        if conn is None:
            if self.connection is None:
                raise ValueError('No connection available')
            return self.connection
        return conn

    def path(self):
        return 'classes/' + self.class_name

    def data(self):
        data = dict(self)
        if self.acl is not None:
            data['ACL'] = self.acl.data()
        return data

    def parse(self, data):
        assert isinstance(data, dict)
        for (key, value) in data.items():
            self[key] = value
        self.id = self.pop('objectId', None)

        # Parse object ACL
        acl_dict = self.pop('ACL', None)
        if acl_dict is not None:
            self.acl = Acl()
            self.acl.parse(acl_dict)

        # Get rid of unused fields
        self.pop('createdAt', None)
        self.pop('updatedAt', None)

    def create(self, conn=None):
        conn = self._check_conn(conn)
        result = conn.post(self.path(), self.data())
        self.parse(result)

    def read(self, conn=None):
        if not self.id:
            raise ParseException('No valid ID to read')
        conn = self._check_conn(conn)
        result = conn.get(self.path() + '/' + self.id)
        self.parse(result)

    # Cannot use update since it is taken by dict.
    def modify(self, conn=None):
        if not self.id:
            raise ParseException('No valid ID to update')
        conn = self._check_conn(conn)
        conn.put(self.path() + '/' + self.id, self.data())

    def delete(self, conn=None):
        if not self.id:
            raise ParseException('No valid ID to delete')
        conn = self._check_conn(conn)
        conn.delete(self.path() + '/' + self.id)