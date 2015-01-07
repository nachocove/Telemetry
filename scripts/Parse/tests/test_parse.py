import unittest

from Parse.connection import Connection
from Parse.exception import ParseException
from Parse.acl import Acl
from Parse.objects import Object
from Parse.users import User



# For PythonUnitTest app
# APP_ID = 'D4Wb9PGYb9gSXNa6Te4Oy31QF7ANnE4uAA9S9F4G'
# REST_API_KEY = '0xH5KQTdOGnzB8sXcfwAmIrSNJYnsuYrO8ZPuzbt'
APP_ID = None
REST_API_KEY = None

class ParseUnitTest(unittest.TestCase):
    pass

@unittest.skipIf(APP_ID is None or REST_API_KEY is None, "Needs active APP_ID and REST_API_KEY")
class TestAcl(ParseUnitTest):
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


@unittest.skipIf(APP_ID is None or REST_API_KEY is None, "Needs active APP_ID and REST_API_KEY")
class TestObject(ParseUnitTest):
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


@unittest.skipIf(APP_ID is None or REST_API_KEY is None, "Needs active APP_ID and REST_API_KEY")
class TestUser(ParseUnitTest):
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


if __name__ == '__main__':
    unittest.main()