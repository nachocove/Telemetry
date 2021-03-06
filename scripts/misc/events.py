TYPES = ['DEBUG',
         'INFO',
         'WARN',
         'ERROR',
         'WBXML_REQUEST',
         'WBXML_RESPONSE',
         'COUNTER',
         'CAPTURE',
         'UI',
         'SUPPORT']

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
                'build_number',
                'build_version',
                'os_type',
                'os_version',
                'device_model']

# These fields are arranged in the order they intend to be displayed
INFO_FIELDS = ['message',  # for logs
               'thread_id',

               'wbxml',  # for wbxml requests / responses

               'counter_name',  # for counters
               'count',
               'counter_start',
               'counter_end',

               'capture_name',   # for captures
               'average',
               'max',
               'min',
               'stddev',

               'ui_type',  # for UI
               'ui_object',
               'ui_string',
               'ui_integer',

               'support',  # for SUPPORT
               ]

VALID_FIELDS = ['timestamp', 'event_type'] + IDENT_FIELDS + INFO_FIELDS + INTERNAL_FIELDS

QUERY_FIELDS = {'average': 'integer',
                'build_version': 'string',
                'capture_name': 'string',
                'client': 'string',
                'counter_name': 'string',
                'count': 'integer',
                'counter_start': 'iso8601',
                'counter_end': 'iso8601',
                'createdAt': 'iso8601',
                'device_model': 'string',
                'event_type': 'string',
                'message': 'string',
                'max': 'integer',
                'min': 'integer',
                'objectId': 'string',
                'os_type': 'string',
                'os_version': 'string',
                'stddev': 'integer',
                'support': 'string',
                'timestamp': 'iso8601',
                'ui_type': 'string',
                'ui_object': 'string',
                'ui_string': 'string',
                'ui_integer': 'integer',
                }