# Copyright 2014, NachoCove, Inc
import ConfigParser
import os
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.s3.connection import S3Connection
from AWS.tables import TelemetryTable

# Get the list of project
projects_cfg = ConfigParser.ConfigParser()
projects_cfg.read('projects.cfg')
projects = projects_cfg.sections()
if not projects:
    raise ValueError('No projects defined')
default_project = os.environ.get('PROJECT', projects[0])

BOTO_DEBUG=False

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
                                                            is_secure=True,
                                                            debug=2 if BOTO_DEBUG else 0)
    TelemetryTable.PREFIX = project
    return _aws_connection_cache[project]


_aws_s3_connection_cache = {}
def aws_s3_connection(project):
    """
    :return: boto.s3.connection.S3Connection
    """
    global _aws_s3_connection_cache
    if not project in _aws_s3_connection_cache:
        if not project in projects:
            raise ValueError('Project %s is not present in projects.cfg' % project)
        _aws_s3_connection_cache[project] = S3Connection(host='s3-us-west-2.amazonaws.com',
                                                         port=443,
                                                         aws_secret_access_key=projects_cfg.get(project, 'secret_access_key'),
                                                         aws_access_key_id=projects_cfg.get(project, 'access_key_id'),
                                                         is_secure=True,
                                                         debug=2 if BOTO_DEBUG else 0)
    TelemetryTable.PREFIX = project
    return _aws_s3_connection_cache[project]
