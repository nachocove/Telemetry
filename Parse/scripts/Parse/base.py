from exception import ParseException


class ObjectBase:
    def __init__(self, conn=None):
        self.connection = conn
        self.id = None

    def __str__(self):
        return str(self.data())

    def _check_conn(self, conn):
        if conn is None:
            if self.connection is None:
                raise ValueError('No connection available')
            return self.connection
        return conn

    def data(self):
        raise ParseException('Unimplemented method')

    def parse(self, data):
        raise ParseException('Unimplemented method')

    def create(self, conn=None):
        conn = self._check_conn(conn)
        result = conn.post(self.path, self.data())
        self.parse(result)

    def read(self, conn=None):
        conn = self._check_conn(conn)
        result = conn.get(self.path + '/' + self.id)
        self.parse(result)

    def update(self, conn=None):
        conn = self._check_conn(conn)
        result = conn.put(self.path + '/' + self.id, self.data())

    def delete(self):
        conn = self._check_conn(conn)
        conn.delete(self.path + '/' + self.id)