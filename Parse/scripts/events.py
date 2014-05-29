TYPES = ['DEBUG',
         'INFO',
         'WARN',
         'ERROR',
         'WBXML_REQUEST',
         'WBXML_RESPONSE',
         'COUNTER',
         'CAPTURE']

# All fields of an event are divided into the following groups
# 1. timestamp
# 2. event_type
# 3. Identification fields - fields that identify device / client version
# 4. Information fields - fields that tell what the client is doing
# 5. Internal fields - fields that are provided by Parse. They can be queried
#    but cannot be used as part of 'keys'.

INTERNAL_FIELDS = ['createdAt',
                   'objectId',
                   'updatedAt']

# These fields are arranged in the order they intend to be displayed
IDENT_FIELDS = ['client',
                'build_version',
                'os_type',
                'os_version',
                'device_model']

# These fields are arranged in the order they intend to be displayed
INFO_FIELDS = ['message',  # for logs

               'wbxml',  # for wbxml requests / responses

               'counter_name',  # for counters
               'count',
               'counter_start',
               'counter_end',

               'capture_name',   # for captures
               'average',
               'max',
               'min']

VALID_FIELDS = ['timestamp', 'event_type'] + IDENT_FIELDS + INFO_FIELDS + INTERNAL_FIELDS