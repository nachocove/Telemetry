# This module provides Parse application keys management using python
# configuration file. 'options' is a argparse parse output. It ia assumed
# to have 4 attributes - app_id, api_key, sessions_token, config
#
# Note that master key is never cached into the configuration file for
# security reason.
import ConfigParser
import os.path


def read_config(options):
    """
    Read the configuration file and set the keys back into the options object.
    """
    config = ConfigParser.RawConfigParser()
    if not os.path.exists(options.config):
        return
    config.read(options.config)
    if config.has_section('keys'):
        if config.has_option('keys', 'app_id'):
            options.app_id = config.get('keys', 'app_id')
        if config.has_option('keys', 'api_key'):
            options.api_key = config.get('keys', 'api_key')
        if config.has_option('keys', 'session_token'):
            options.session_token = config.get('keys', 'session_token')


def write_config(options):
    """
    Write the configuration file from the options object
    """
    config = ConfigParser.RawConfigParser()
    if os.path.exists(options.config):
        config.read(options.config)
    if not config.has_section('keys'):
        config.add_section('keys')
    if options.app_id is not None:
        config.set('keys', 'app_id', options.app_id)
    if options.api_key is not None:
        config.set('keys', 'api_key', options.api_key)
    if options.session_token is not None:
        config.set('keys', 'session_token', options.session_token)
    with open(options.config, 'w') as f:
        config.write(f)
