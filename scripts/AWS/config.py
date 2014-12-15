from misc.config import SectionConfig


class AwsConfig(SectionConfig):
    SECTION = 'aws'
    KEYS = (
        'access_key_id',
        'account_id',
        'identity_pool_id',
        'prefix',
        'secret_access_key',
        's3_bucket',
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)

class CliFunc(object):
    def add_arguments(self, parser, subparser):
        pass

    def run(self, args, **kwargs):
        pass
