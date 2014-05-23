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

    def read_keys(self, options):
        """
        Read the Parse keys and set them back to the options object.
        """
        if not self.config.has_section('keys'):
            return
        if self.config.has_option('keys', 'app_id'):
            options.app_id = self.config.get('keys', 'app_id')
        if self.config.has_option('keys', 'api_key'):
            options.api_key = self.config.get('keys', 'api_key')
        if self.config.has_option('keys', 'session_token'):
            options.session_token = self.config.get('keys', 'session_token')

    def write_keys(self, options):
        """
        Write the keys back to the configuration file.
        """
        if not self.config.has_section('keys'):
            self.config.add_section('keys')
        if options.app_id is not None:
            self.config.set('keys', 'app_id', options.app_id)
        if options.api_key is not None:
            self.config.set('keys', 'api_key', options.api_key)
        if options.session_token is not None:
            self.config.set('keys', 'session_token', options.session_token)
        self.write()

    def write(self):
        with open(self.cfg_file, 'w') as f:
            self.config.write(f)