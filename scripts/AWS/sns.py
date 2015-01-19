# Copyright 2014, NachoCove, Inc
import json

import logging
from M2Crypto import X509, EVP
from AWS.cognito import Boto3CliFunc

logger = logging.getLogger('cognito-setup')

class ListApplicationARNs(Boto3CliFunc):
    def add_arguments(self, parser, subparser):
        list_parser = subparser.add_parser('list-sns-platform-apps')
        list_parser.add_argument('--region', '-r',
                                help='AWS Region Name',
                                dest='region',
                                default='us-west-2')
        list_parser.add_argument('--boto-debug', help='Debug output from boto/botocore', action='store_true', default=False)
        list_parser.add_argument('--list-clients', action="store_true", help="List all clients attached to an application ARN", default=False)
        list_parser.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(ListApplicationARNs, self).run(args, **kwargs)
        conn = self.session.client('sns')
        logger.setLevel(logging.INFO)
        apps = conn.list_platform_applications()
        for app in apps['PlatformApplications']:
            print "%(PlatformApplicationArn)s: %(Attributes)s" % app
            if args.list_clients:
                clients = conn.list_endpoints_by_platform_application(PlatformApplicationArn=app['PlatformApplicationArn'])
                for client in clients['Endpoints']:
                    print "\t%(EndpointArn)s: %(Attributes)s" % client

class CreateApplicationARN(Boto3CliFunc):
    def add_arguments(self, parser, subparser):
        add_parser = subparser.add_parser('create-sns-platform-app')
        add_parser.add_argument('--region', '-r',
                                help='AWS Region Name',
                                dest='region',
                                default='us-west-2')
        add_parser.add_argument('--boto-debug', help='Debug output from boto/botocore', action='store_true', default=False)
        add_parser.add_argument('push-service', choices=('APNS',), help="The push service", default=None)
        add_parser.add_argument('name', help="The Platform Application name")
        add_parser.add_argument('push-creds', help="Push Service credentials. Dependent on push-service", default=None)
        add_parser.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(CreateApplicationARN, self).run(args, **kwargs)
        conn = self.session.client('sns')
        logger.setLevel(logging.INFO)
        if args.push_service == 'APNS':
            creds = json.loads(args.push_service)
            attributes = {'PlatformPrincipal': open(creds['key']).read(),
                          'PlatformCredential': open(creds['cert']).read(),
                      }
        else:
            raise ValueError("Not support push platform: %s" % args.push_service)

        response = conn.create_platform_application(Name=args.name, Platform=args.push_service, Attributes=attributes)
        print response
