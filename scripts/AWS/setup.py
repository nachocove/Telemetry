#!/usr/bin/env python
from __future__ import absolute_import
import os
import sys
from argparse import ArgumentParser
from AWS.sns import ListApplicationARNs

sys.path.append('../')
from AWS import cognito, dynamoDB, config
from misc.config import Config

def main():
    cliFuncs = {'dynamodb': (dynamoDB.ListTables(),
                             dynamoDB.DeleteTables(),
                             dynamoDB.CreateTables(),
                             dynamoDB.ShowTableCost(),
                             ),
                'cognito': (cognito.SetupPool(),
                            cognito.DeletePools(),
                            cognito.ListPools(),
                            cognito.TestAccess(),
                            ),
                'sns': (ListApplicationARNs(),)
                }
    parser = ArgumentParser(add_help=False)
    parser.add_argument('--secret-access-key', '-s',
                            help='AWS secret access key',
                            dest='aws_secret_access_key',
                            default=None)
    parser.add_argument('--access-key-id', '-a',
                            help='AWS access key id',
                            dest='aws_access_key_id',
                            default=None)
    parser.add_argument('--prefix', '-p',
                            help='Prefix of the table names',
                            default='', dest='aws_prefix')
    parser.add_argument('--host',
                        help='Host of AWS DynamoDB instance (Default: dynamodb.us-west-2.amazonaws.com)',
                        default='dynamodb.us-west-2.amazonaws.com')
    parser.add_argument('--port',
                        help='Port of AWS DynamoDB instance (Default: 443)',
                        default=443)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--local',
                       help='Use local DynamoDB',
                       action='store_true',
                       default=False)
    group.add_argument('--config', '-c',
                       help='Configuration that contains an AWS section',
                       default=None)


    subparser = parser.add_subparsers()
    for k in cliFuncs:
        for c in cliFuncs[k]:
            c.add_arguments(parser, subparser)

    options = parser.parse_args()

    if options.config:
        if not os.path.exists(options.config):
            print "ERROR: config file %s does not exist" % options.config
            sys.exit(1)

        config_file = Config(options.config)
        config.AwsConfig(config_file).read(options)

    if not options.aws_access_key_id or not options.aws_secret_access_key:
        print "ERROR: No access-key or secret key. Need either a config or aws_access_key_id and aws_secret_access_key."
        sys.exit(1)

    if not options.func:
        raise ValueError('no function defined for argument')

    options.func(options)

if __name__ == '__main__':
    main()
