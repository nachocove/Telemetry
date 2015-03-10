# Copyright 2014, NachoCove, Inc


from misc.config import SectionConfig


class FreshdeskConfig(SectionConfig):
    SECTION = 'freshdesk'
    KEYS = (
        'api_key',
        'priority',
        'cc_emails',
        'hostname',
    )

    DEFAULTS = {'priority': 1,
                'cc_emails': None,
                'hostname': None,
                }
    def __init__(self, config_file):
        super(FreshdeskConfig, self).__init__(config_file)

    def read(self, options):
        super(FreshdeskConfig, self).read(options)
        for k in self.KEYS:
            attr = "%s_%s" % (self.SECTION, k)
            if not hasattr(options, attr) and k in self.DEFAULTS:
                setattr(options, attr, self.DEFAULTS[k])

