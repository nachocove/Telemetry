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
    class FileNotFoundException(Exception):
        pass

    def __init__(self, cfg_file, create=False):
        self.cfg_file = cfg_file
        self.config = ConfigParser.RawConfigParser()
        if os.path.exists(self.cfg_file):
            self.config.read(self.cfg_file)
        elif not create:
            raise self.FileNotFoundException(cfg_file)

    def _section_and_key_exist(self, section, key):
        if not self.config.has_section(section):
            return False
        if not self.config.has_option(section, key):
            return False
        return True

    def get(self, section, key):
        if not self._section_and_key_exist(section, key):
            return None
        return self.config.get(section, key)

    def getbool(self, section, key):
        if not self._section_and_key_exist(section, key):
            return None
        return self.config.getboolean(section, key)

    def getint(self, section, key):
        if not self._section_and_key_exist(section, key):
            return None
        return self.config.getint(section, key)

    def set(self, section, key, value):
        if value is None:
            return
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, key, value)

    def write(self):
        with open(self.cfg_file, 'w') as f:
            self.config.write(f)


class SectionConfig(object):
    SECTION = None
    KEYS = None

    def __init__(self, config_file):
        assert isinstance(config_file, Config)
        self.config_file = config_file

    def __getattr__(self, key):
        if key == 'config_file':
            return self.__dict__[key]
        cls = self.__class__
        if key not in cls.KEYS:
            raise AttributeError('Unknown attribute %s' % key)
        return self.config_file.get(cls.SECTION, key)

    def __setattr__(self, key, value):
        if key == 'config_file':
            self.__dict__[key] = value
            return
        cls = type(self)
        if key not in cls.KEYS:
            raise AttributeError('Unknown attribute %s' % key)
        self.config_file.set(cls.SECTION, key, value)

    def read(self, options):
        cls = self.__class__
        for key in cls.KEYS:
            value = getattr(self, key)
            if value is not None:
                setattr(options, cls.SECTION + '_' + key, value)

class ColorsConfig(SectionConfig):
    SECTION = 'colors'
    KEYS = (
        'warn',
        'error',
        'wbxml_request',
        'wbxml_response',
        'counter',
        'capture',
        'support',
        'ui'
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)


class WbxmlToolConfig(SectionConfig):
    SECTION = 'wbxml_tool'
    KEYS = (
        'wbxml_tool_path'
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)
