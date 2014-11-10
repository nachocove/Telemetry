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

    def get(self, section, key):
        if not self.config.has_section(section):
            return None
        if not self.config.has_option(section, key):
            return None
        return self.config.get(section, key)

    def set(self, section, key, value):
        if value is None:
            return
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, key, value)

    def write(self):
        with open(self.cfg_file, 'w') as f:
            self.config.write(f)

    def read_wbxml_tool(self, options):
        """
        Read the path to WbxmlTool
        """
        options.wbxml_tool_path = self.get('wbxml_tool', 'wbxml_tool_path')

    def write_wbxml_tool(self, options):
        """
        Write the path to WbxmlTool
        """
        self.set('wbxml_tool', 'wbxml_tool_path', options.wbxml_tool_path)
        self.write()


class SectionConfig:
    SECTION = None
    KEYS = None

    def __init__(self, config_file):
        assert isinstance(config_file, Config)
        self.config_file = config_file

    def __getattr__(self, key):
        cls = type(self)
        if key not in cls.KEYS:
            raise AttributeError('Unknown attribute %s' % key)
        return self.config_file.get(cls.SECTION, key)

    def __setattr__(self, key, value):
        cls = type(self)
        if key not in cls.KEYS:
            raise AttributeError('Unknown attribute %s' % key)
        self.config_file.set(cls.SECITON, key, value)

    def read(self, options):
        cls = type(self)
        for key in cls.KEYS:
            value = getattr(self, key)
            if value is not None:
                setattr(options, key, value)


class EmailConfig(SectionConfig):
    SECTION = "email"
    KEYS = (
        'smtp_server',
        'port',
        'start_tls',
        'username',
        'recipient'
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)
