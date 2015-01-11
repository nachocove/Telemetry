import unittest
from HockeyApp.hockeyapp import HockeyApp
from HockeyApp.app import App
from HockeyApp.version import Version

@unittest.skip('Needs active api-token matching an app')
class TestHockeyApp(unittest.TestCase):
    def setUp(self):
        self.ha_obj = HockeyApp(api_token='4a472c5e774a4004a4eb1dd648b8af8a')

    @staticmethod
    def remove_nachomail_app(app_list):
        for app in app_list:
            if app.title != 'NachoMail':
                continue
            app_list.remove(app)

    def test_app(self):
        app_list = self.ha_obj.apps()
        TestHockeyApp.remove_nachomail_app(app_list)
        self.assertEqual(app_list, [])

        self.app = App(self.ha_obj,
                       title='TestApp',
                       bundle_id='com.nachocove.testapp',
                       platform='iOS',
                       release_type='alpha')
        self.app.create()

        app_list = self.ha_obj.apps()
        self.remove_nachomail_app(app_list)
        self.assertEqual(len(app_list), 1)
        self.assertEqual(app_list[0], self.app)

        self.app.delete()

        app_list = self.ha_obj.apps()
        TestHockeyApp.remove_nachomail_app(app_list)
        self.assertEqual(app_list, [])

    def test_version(self):
        self.app = App(self.ha_obj,
                       title='TestApp',
                       bundle_id='com.nachocove.testapp',
                       platform='iOS',
                       release_type='alpha')
        self.app.create()

        version_list = self.app.versions()
        self.assertEqual(version_list, [])

        self.version = Version(self.app,
                               version='4.1',
                               short_version='123')
        self.version.create()

        version_list = self.app.versions()
        self.assertEqual(len(version_list), 1)
        self.assertEqual(version_list[0], self.version)

        self.version.delete()

        version_list = self.app.versions()
        self.assertEqual(version_list, [])

        self.app.delete()

if __name__ == '__main__':
    unittest.main()