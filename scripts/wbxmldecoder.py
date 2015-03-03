# Copyright 2014, NachoCove, Inc
from PyWBXMLDecoder.ASCommandResponse import ASCommandResponse
import base64
import sys

try:
    raw_data = base64.b64decode(sys.argv[1])
except Exception as e:
    print "ERROR: Could not base64 decode the data"
    sys.exit(1)

try:
    response = ASCommandResponse(raw_data).xmlString
    if response is None:
        raise Exception("Could not process WBXML response")
except Exception as e:
    print "ERROR: Could not decode wbxml. Base64 decode follows:\n%s\n" % raw_data
    sys.exit(1)
sys.exit(0)

