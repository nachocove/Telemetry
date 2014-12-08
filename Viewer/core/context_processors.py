# Copyright 2014, NachoCove, Inc


import os

def process_globals(request):
    return {'project': os.environ.get('PROJECT', '<not set>')}
