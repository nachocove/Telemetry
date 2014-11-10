from misc.config import SectionConfig


class AwsConfigSec(SectionConfig):
    SECTION = 'AWS'
    KEYS = (
        'access_key_id',
        'account_id',
        'identiy_pool_id',
        'secret_access_key'
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)
