import unittest
import os
from config import Config


class MockOptions:
    def __init__(self, app_id=None, api_key=None, session_token=None, wbxml_tool_path=None):
        self.app_id = app_id
        self.api_key = api_key
        self.session_token = session_token
        self.wbxml_tool_path = wbxml_tool_path

    def __eq__(self, other):
        return (self.app_id == other.app_id and
                self.api_key == other.api_key and
                self.session_token == other.session_token and
                self.wbxml_tool_path == other.wbxml_tool_path)


class TestConfig(unittest.TestCase):
    config_filename = 'config_unit_test.cfg'

    def setUp(self):
        self.options = MockOptions()

    def tearDown(self):
        try:
            os.unlink(TestConfig.config_filename)
        except OSError:
            pass

    def write_config(self, config):
        with open(self.config_filename, 'w') as f:
            f.write(config)

    def read_config(self):
        try:
            with open(self.config_filename, 'r') as f:
                got = f.read()
                return got
        except IOError:
            return None

    def test_read_no_file(self):
        config = Config(self.config_filename)
        config.read_keys(self.options)
        expected = MockOptions(app_id=None, api_key=None, session_token=None)
        self.assertEqual(expected, self.options)

    def test_read_file_no_section(self):
        self.write_config('')
        config = Config(self.config_filename)
        config.read_keys(self.options)
        expected = MockOptions(app_id=None, api_key=None, session_token=None)
        self.assertEqual(expected, self.options)

    def test_read_file_section(self):
        self.write_config('[keys]')
        config = Config(self.config_filename)
        config.read_keys(self.options)
        expected = MockOptions(app_id=None, api_key=None, session_token=None)
        self.assertEqual(expected, self.options)

    def test_read_2keys(self):
        self.write_config('[keys]\n'
                          'app_id = abcefg\n'
                          'session_token = 012345678\n\n')
        config = Config(self.config_filename)
        config.read_keys(self.options)
        expected = MockOptions(app_id='abcefg', api_key=None, session_token='012345678')
        self.assertEqual(expected, self.options)

    def test_read_3keys(self):
        self.write_config('[keys]\n'
                          'app_id = abcdefg\n'
                          'api_key = xyz\n'
                          'session_token = 012345678\n\n')
        config = Config(self.config_filename)
        config.read_keys(self.options)
        expected = MockOptions(app_id='abcdefg', api_key='xyz', session_token='012345678')
        self.assertEqual(expected, self.options)

    def test_read_wbxml_tool_path(self):
        self.write_config('[wbxml_tool]\n'
                          'wbxml_tool_path = /Users/bob/WbxmlTool.Mac.exe')
        config = Config(self.config_filename)
        config.read_wbxml_tool(self.options)
        expected = MockOptions(wbxml_tool_path='/Users/bob/WbxmlTool.Mac.exe')
        self.assertEqual(expected, self.options)

    def test_write_no_keys(self):
        config = Config(self.config_filename)
        config.write_keys(self.options)
        expected = ''
        got = self.read_config()
        self.assertEqual(expected, got)

    def test_write_session_token(self):
        self.options.session_token = '012345678'
        config = Config(self.config_filename)
        config.write_keys(self.options)
        expected = '[keys]\n' \
                   'session_token = 012345678\n\n'
        got = self.read_config()
        self.assertEqual(expected, got)

    def test_write_3keys(self):
        self.options.app_id = 'abcdefg'
        self.options.api_key = 'xyz'
        self.options.session_token = '012345678'
        config = Config(self.config_filename)
        config.write_keys(self.options)
        expected = '[keys]\n' \
                   'app_id = abcdefg\n' \
                   'api_key = xyz\n' \
                   'session_token = 012345678\n\n'
        got = self.read_config()
        self.assertEqual(expected, got)

    def test_write_wbxml_tool_path(self):
        self.options.wbxml_tool_path = '/Users/bob/WbxmlTool.Mac.exe'
        config = Config(self.config_filename)
        config.write_wbxml_tool(self.options)
        expected = '[wbxml_tool]\n' \
                   'wbxml_tool_path = /Users/bob/WbxmlTool.Mac.exe\n\n'
        got = self.read_config()
        self.assertEqual(expected, got)

if __name__ == '__main__':
    unittest.main()