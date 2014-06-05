import unittest
from connection import Connection
from exception import ParseException
from acl import Acl
from objects import Object
from users import User
from utc_datetime import UtcDateTime


# For PythonUnitTest app
APP_ID = 'D4Wb9PGYb9gSXNa6Te4Oy31QF7ANnE4uAA9S9F4G'
REST_API_KEY = '0xH5KQTdOGnzB8sXcfwAmIrSNJYnsuYrO8ZPuzbt'


class TestAcl(unittest.TestCase):
    def setUp(self):
        self.dict = {'user1': {'read': True},
                     'user2': {'write': True},
                     'user3': {'read': True, 'write': True}}

    def test_data(self):
        acl = Acl()
        acl.add(name='user1', read=True, write=False)
        acl.add(name='user2', read=False, write=True)
        acl.add(name='user3', read=True, write=True)
        self.assertEqual(acl.data(), self.dict)

    def test_parse(self):
        acl = Acl()
        acl.parse(self.dict)
        self.assertEqual(acl.data(), self.dict)


class TestObject(unittest.TestCase):
    def setUp(self):
        self.conn = Connection(app_id=APP_ID, api_key=REST_API_KEY)
        self.dict = {'integer': 123,
                     'boolean': False,
                     'string': 'Hello, World'}

    def test_data(self):
        obj = Object(class_name='Objects')
        obj['integer'] = 123
        obj['boolean'] = False
        obj['string'] = 'Hello, World'
        self.assertEqual(obj.data(), self.dict)

        obj.acl = Acl()
        obj.acl.add(name='user', read=True)
        dict2 = dict()
        dict2.update(self.dict)
        dict2['ACL'] = {'user': {'read': True}}
        self.assertEqual(obj.data(), dict2)

    def test_parse(self):
        obj = Object()
        obj.parse({'integer': 123, 'boolean': False, 'string': 'Hello, World'})
        self.assertEqual(obj['integer'], 123)
        self.assertEqual(obj['boolean'], False)
        self.assertEqual(obj['string'], 'Hello, World')

    def compare_objects(self, obj1, obj2):
        self.assertEqual(obj1, obj2)
        self.assertEqual(obj1.id, obj2.id)
        self.assertEqual(obj1.class_name, obj2.class_name)

    def test_without_acl(self):
        # Create an obj
        obj = Object(conn=self.conn, class_name='Objects')
        obj['integer'] = 123
        obj['boolean'] = False
        obj['string'] = 'Hello, World'
        obj.create()
        self.assertIsNotNone(obj.id)

        # Read the object back
        obj2 = Object(conn=self.conn, class_name='Objects')
        obj2.id = obj.id
        obj2.read()

        self.compare_objects(obj, obj2)

        # Query all
        obj2 = Object.query('Objects', conn=self.conn)
        self.assertEqual(len(obj2), 1)
        self.compare_objects(obj, obj2[0])

        # Query by object id
        obj2 = Object.query('Objects', conn=self.conn, obj_id=obj.id)
        self.compare_objects(obj, obj2)

        # Update it
        obj['integer'] = 321
        obj['boolean'] = True
        obj.modify()

        obj2 = Object(conn=self.conn, class_name='Objects')
        obj2.id = obj.id
        obj2.read()

        self.compare_objects(obj, obj2)

        # Delete it
        obj.delete()
        self.assertRaises(ParseException, obj.delete)
        self.assertRaises(ParseException, obj.read)

    def test_with_acl(self):
        # Create an object with ACL
        obj = Object(conn=self.conn, class_name='Objects')
        obj['integer'] = 111
        obj.acl = Acl()
        obj.acl.add('*', read=True, write=True)
        obj.create()
        self.assertIsNotNone(obj.id)

        # Read it and verify the ACL is read correctly
        obj2 = Object(conn=self.conn, class_name='Objects')
        obj2.id = obj.id
        obj2.read()

        self.assertEqual(obj, obj2)
        self.assertEqual(obj.id, obj2.id)
        self.assertEqual(obj.class_name, obj2.class_name)
        self.assertEqual(obj.acl, obj2.acl)

        obj.delete()
        self.assertRaises(ParseException, obj.delete)
        self.assertRaises(ParseException, obj.read)


class TestUser(unittest.TestCase):
    def setUp(self):
        self.conn = Connection(app_id=APP_ID, api_key=REST_API_KEY)
        self.username = 'bip cotton'
        self.password = '123$<>!'

    def compare_users(self, user1, user2):
        # User comparison is weird because REST API will never return the password on read
        self.assertEqual(user1.id, user2.id)
        self.assertEqual(user1.class_name, user2.class_name)
        self.assertEqual(user1['username'], user2['username'])
        self.assertEqual(user1['email'], user2['email'])
        self.assertEqual(user1.session_token, user2.session_token)

    def test_user(self):
        # Create a user
        user = User(conn=self.conn, username=self.username, password=self.password, email='nobody@company.com')
        user.create()
        self.assertIsNotNone(user.id)
        self.assertIsNotNone(user.session_token)

        # Read it
        conn2 = Connection(app_id=APP_ID, api_key=REST_API_KEY, session_token=user.session_token)
        user2 = User(conn=conn2)
        user2.id = user.id
        user2.read()

        self.compare_users(user, user2)

        # Login
        user2 = User.login(username=self.username, password=self.password, conn=self.conn)

        self.compare_users(user, user2)

        # Modify its email address. To do this, we need a new connection with session token
        user['email'] = 'somebody@company.com'
        user.modify(conn2)

        user2 = User(conn=conn2)
        user2.id = user.id
        user2.read()

        self.compare_users(user, user2)

        # Delete it
        user.delete(conn2)
        self.assertRaises(ParseException, user.delete)
        self.assertRaises(ParseException, user.read)


class TestUtcDateTime(unittest.TestCase):
    def setUp(self):
        self.test_vectors = ['2014-06-05T01:02:03.004Z',
                             '2013-11-30T23:59:40.111Z']

    def test_decode(self):
        """
        Convert str to UtcDateTime object.
        """
        dt1 = UtcDateTime(self.test_vectors[0])
        self.assertEqual(dt1.datetime.year, 2014)
        self.assertEqual(dt1.datetime.month, 6)
        self.assertEqual(dt1.datetime.day, 5)
        self.assertEqual(dt1.datetime.hour, 1)
        self.assertEqual(dt1.datetime.minute, 2)
        self.assertEqual(dt1.datetime.second, 3)
        self.assertEqual(dt1.datetime.microsecond, 4000)

        dt2 = UtcDateTime(self.test_vectors[1])
        self.assertEqual(dt2.datetime.year, 2013)
        self.assertEqual(dt2.datetime.month, 11)
        self.assertEqual(dt2.datetime.day, 30)
        self.assertEqual(dt2.datetime.hour, 23)
        self.assertEqual(dt2.datetime.minute, 59)
        self.assertEqual(dt2.datetime.second, 40)
        self.assertEqual(dt2.datetime.microsecond, 111000)

    def test_encode(self):
        """
        Convert UtcDateTime object to str.
        """
        for dt_str in self.test_vectors:
            dt = UtcDateTime(dt_str)
            self.assertEqual(str(dt), dt_str)

    def test_sub(self):
        """
        Test __sub__ works. There are a lot of tricky case involed in leap years.
        I'm going to ignore those as the elepase time in most case should be no
        more than a few hours to a day.
        """
        start = UtcDateTime('2014-06-05T01:02:04.001Z')
        end1 = UtcDateTime('2014-06-05T01:02:04.999Z')
        self.assertEqual(end1 - start, 0.998)

        end2 = UtcDateTime('2014-06-05T01:02:05.001Z')
        self.assertEqual(end2 - start, 1.0)

        end3 = UtcDateTime('2014-06-05T01:03:04.001Z')
        self.assertEqual(end3 - start, 60.0)

        end4 = UtcDateTime('2014-06-05T02:02:04.001Z')
        self.assertEqual(end4 - start, 3600.0)

        end5 = UtcDateTime('2014-06-06T01:02:04.001Z')
        self.assertEqual(end5 - start, 86400.0)

        end6 = UtcDateTime('2014-07-05T01:02:04.001Z')
        self.assertEqual(end6 - start, 86400.0 * 30)

        end7 = UtcDateTime('2015-06-05T01:02:04.001Z')
        self.assertEqual(end7 - start, 86400.0 * 365)

        end8 = UtcDateTime('2015-07-06T02:03:05.999Z')
        total = (86400.0 * (365 + 30 + 1)) + 3600.0 + 60.0 + 1.0 + 0.998
        self.assertEqual(end8 - start, total)
        self.assertEqual(start - end8, -total)


if __name__ == '__main__':
    unittest.main()