from misc.config import SectionConfig


class AwsConfig(SectionConfig):
    SECTION = 'aws'
    KEYS = (
        'access_key_id',
        'account_id',
        'identity_pool_id',
        'prefix',
        'secret_access_key',
        'client_data_bucket',
        'client_data_prefix',
        'sns_platform_app_arn',
        'telemetry_bucket',
        'telemetry_prefix',
        'log_t3_bucket',
        'device_info_t3_bucket',
        'support_t3_bucket',
        'pinger_t3_bucket',
        'isT3',
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)

    def __getattr__(self, key):
        if key == 'isT3':
            return self.config_file.getbool(AwsConfig.SECTION, key)
        return SectionConfig.__getattr__(self, key)

class CliFunc(object):
    def add_arguments(self, parser, subparser):
        pass

    def run(self, args, **kwargs):
        pass
