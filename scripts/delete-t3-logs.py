# Copyright 2014, NachoCove, Inc
import argparse
import logging
import os

import sys

from datetime import timedelta

from AWS.config import AwsConfig
from AWS.s3_telemetry import create_s3_conn, delete_s3_events
from misc.config import Config
from misc.utc_datetime import UtcDateTime


class DateTimeAction(argparse.Action):
    """
    This class parses the ISO-8601 UTC time given in --after and --before.
    """

    def __call__(self, parser, namespace, value, option_string=None):
        if (option_string != '--after') and (option_string != '--before'):
            raise ValueError('unexpected option %s with datetime ' % option_string)
        if (option_string == '--after') and ('last' == value):
            setattr(namespace, self.dest, 'last')
        elif (option_string == '--before') and (value.startswith('now')):
            setattr(namespace, self.dest, UtcDateTime(value))
        else:
            try:
                setattr(namespace, self.dest, UtcDateTime(value))
            except Exception:
                raise argparse.ArgumentError(self, "not a valid Date-time argument: %s" % value)

bucket_suffixes = ["counter", "device-info", "distribution", "log", "protocol", "samples", "statistics2", "support", "time-series", "trouble-tickets"]

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-v', '--verbose',
                        help='Verbose',
                        action='store_true',
                        default=False)
    parser.add_argument('-d', '--debug',
                        help='Debug',
                        action='store_true',
                        default=False)
    parser.add_argument('--debug-boto',
                        help='Debug Boto',
                        action='store_true',
                        default=False)
    parser.add_argument("--aws-access-key-id", help="AWS Access Key Id")
    parser.add_argument("--aws-secret-access-key", help="AWS Secret key")
    parser.add_argument("--aws-security-token", help="AWS Securty Token")
    parser.add_argument("--aws-bucket-prefix", help="AWS bucket name prefix")
    parser.add_argument("--aws-project", help="AWS project prefix")
    parser.add_argument('--after',
                        help='Time window starting time in ISO-8601 UTC or "last" for the last saved time',
                        action=DateTimeAction,
                        dest='start',
                        default=None)
    parser.add_argument('--before',
                        help='Time window ending time in ISO-8601 UTC or "now" for the current time',
                        action=DateTimeAction,
                        dest='end',
                        default=None)

    options = parser.parse_args()

    logging_format = '%(asctime)s.%(msecs)03d  %(levelname)-8s %(message)s'
    logger = logging.getLogger('monitor')

    logger.setLevel(logging.INFO)
    if options.debug or options.verbose:
        streamhandler = logging.StreamHandler(sys.stdout)
        streamhandler.setLevel(logging.DEBUG if options.debug else logging.INFO)
        streamhandler.setFormatter(logging.Formatter(logging_format))
        logger.addHandler(streamhandler)

    if not options.start or not options.end or not options.aws_bucket_prefix or not options.aws_project:
        parser.print_help()
        exit(0)

    if options.debug:
        logger.setLevel(logging.DEBUG)
    elif options.verbose:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)

    key_id = options.aws_access_key_id if options.aws_access_key_id else os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = options.aws_secret_access_key if options.aws_secret_access_key else os.environ.get("AWS_SECRET_ACCESS_KEY")
    security_token = options.aws_security_token if options.aws_security_token else os.environ.get("AWS_SECURITY_TOKEN")
    s3conn = create_s3_conn(key_id, secret_key, security_token, debug=options.debug_boto)

    delta = options.end.datetime - options.start.datetime
    prefixes = [(options.start.datetime + timedelta(days=x)).strftime("%Y%m%d") for x in range(0, delta.days)]

    for bucket in [ "%s-%s-t3-%s" % (options.aws_bucket_prefix, options.aws_project, x) for x in bucket_suffixes]:
        logger.info("Processing bucket %s", bucket)
        try:
            if delete_s3_events(s3conn, bucket, prefixes, options.start, options.end, logger=logger):
                logger.info("Success: %s", bucket)
            else:
                logger.error("ERROR: %s", bucket)
                sys.exit(0)
        except Exception as e:
            logger.error("ERROR: %s %s", bucket, e)


if __name__ == '__main__':
    main()
