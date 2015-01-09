# Copyright 2014, NachoCove, Inc
import ConfigParser
import os
from boto.dynamodb2.layer1 import DynamoDBConnection
from AWS.tables import TelemetryTable

# Get the list of project
projects_cfg = ConfigParser.ConfigParser()
projects_cfg.read('projects.cfg')
projects = projects_cfg.sections()
if not projects:
    raise ValueError('No projects defined')
default_project = os.environ.get('PROJECT', projects[0])


_aws_connection_cache = {}
def aws_connection(project):
    global _aws_connection_cache
    if not project in _aws_connection_cache:
        if not project in projects:
            raise ValueError('Project %s is not present in projects.cfg' % project)
        _aws_connection_cache[project] = DynamoDBConnection(host='dynamodb.us-west-2.amazonaws.com',
                                                            port=443,
                                                            aws_secret_access_key=projects_cfg.get(project, 'secret_access_key'),
                                                            aws_access_key_id=projects_cfg.get(project, 'access_key_id'),
                                                            region='us-west-2',
                                                            is_secure=True)
    TelemetryTable.PREFIX = project
    return _aws_connection_cache[project]


