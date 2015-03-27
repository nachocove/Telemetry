# Copyright 2014, NachoCove, Inc
from PyWBXMLDecoder.ASCommandResponse import ASCommandResponse
import base64
import sys
import os

def decode_and_print(data):
    try:
        raw_data = base64.b64decode(data)
    except Exception as e:
        print "ERROR: Could not base64 decode the data: %s" % e
        return False

    try:
        response = ASCommandResponse(raw_data).xmlString
        if response is None:
            raise Exception("Could not process WBXML response")
        print response
    except Exception as e:
        print "ERROR: Could not decode wbxml. %s\n Base64 decode follows:\n%s\n" % (e, raw_data)
        return False
    return True

def main():
    if sys.argv[1] == "-h":
        print "USAGE: %s <wbxml base64 encoded data or '-' for stdin>" % os.path.basename(sys.argv[0])
        return

    if sys.argv[1] == "-":
        try:
            print "(Ctrl-c or ctrl-d to exit)"
            data = raw_input("WBXML:")
            while data:
                decode_and_print(data)
                data = raw_input("WBXML:")
        except (EOFError, KeyboardInterrupt):
            print
            return
    else:
        decode_and_print(sys.argv[1])

if __name__ == "__main__":
    main()

