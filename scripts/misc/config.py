# This module provides a base class for configuration file management
# It provides Parse application keys management using Python configuration
# file. 'options' is a argparse parse output. It ia assumed to have
# to have 3 attributes - app_id, api_key, sessions_token.
#
# Note that master key is never cached into the configuration file for
# security reason.
#
# Note that each script can extend this to hold additional configuration
# (and maybe even states) on the configuration file.

import ConfigParser
import os.path


class Config:
    def __init__(self, cfg_file):
        self.cfg_file = cfg_file
        self.config = ConfigParser.RawConfigParser()
        if os.path.exists(self.cfg_file):
            self.config.read(self.cfg_file)

    def get(self, section, key, options):
        if not self.config.has_section(section):
            return
        if not self.config.has_option(section, key):
            return
        setattr(options, key, self.config.get(section, key))

    def set(self, section, key, options):
        value = getattr(options, key)
        if value is None:
            return
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, key, value)

    def read_keys(self, options):
        """
        Read the Parse keys and set them back to the options object.
        """
        self.get('keys', 'app_id', options)
        self.get('keys', 'api_key', options)
        self.get('keys', 'session_token', options)

    def write_keys(self, options):
        """
        Write the keys back to the configuration file.
        """
        self.set('keys', 'app_id', options)
        self.set('keys', 'api_key', options)
        self.set('keys', 'session_token', options)
        self.write()

    def write(self):
        with open(self.cfg_file, 'w') as f:
            self.config.write(f)

    def read_wbxml_tool(self, options):
        """
        Read the path to WbxmlTool
        """
        self.get('wbxml_tool', 'wbxml_tool_path', options)

    def write_wbxml_tool(self, options):
        """
        Write the path to WbxmlTool
        """
        self.set('wbxml_tool', 'wbxml_tool_path', options)
        self.write()
