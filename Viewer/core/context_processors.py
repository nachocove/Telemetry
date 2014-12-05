# Copyright 2014, NachoCove, Inc
import os

def globals(request):
    return {'project': os.environ.get('PROJECT')}
