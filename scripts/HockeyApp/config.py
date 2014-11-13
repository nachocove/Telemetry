from misc.config import SectionConfig


class HockeyAppConfig(SectionConfig):
    SECTION = 'hockeyapp'
    KEYS = (
        'app_id',
        'api_token'
    )

    def __init__(self, config_file):
        SectionConfig.__init__(self, config_file)