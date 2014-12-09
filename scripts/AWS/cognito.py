# Copyright 2014, NachoCove, Inc
from argparse import ArgumentParser
import sys
import boto3
from botocore.exceptions import ClientError
from AWS.config import AwsConfig
from misc.config import Config

def list_pools(conn, args):
    identity_pools = conn.list_identity_pools(MaxResults=60)
    for ip in identity_pools['IdentityPools']:
        pool = conn.describe_identity_pool(IdentityPoolId=ip['IdentityPoolId'])
        print "%(IdentityPoolName)s: id=%(IdentityPoolId)s AllowUnauthenticatedIdentities=%(AllowUnauthenticatedIdentities)s" % pool
        if args.list_ids:
            next_token = None
            while True:
                kwargs = {'IdentityPoolId': ip['IdentityPoolId'],
                          'MaxResults': 60}
                if next_token:
                    kwargs['NextToken'] = next_token
                ids = conn.list_identities(**kwargs)
                if not ids:
                    break
                id_list = [x['IdentityId'] for x in ids['Identities']]
                print ",".join(id_list)
                next_token = ids['NextToken']
    return True

def test_access(conn, args):
    anon_session = boto3.session.Session(aws_access_key_id='', aws_secret_access_key='', region_name=args.region)
    anon_session._session.set_credentials(access_key='', secret_key='')
    anon_conn = anon_session.client('cognito-identity')
    response = anon_conn.get_id(AccountId=args.aws_account_id, IdentityPoolId=args.pool_id)
    if not 'IdentityId' in response:
        print response
        return False
    my_id = response['IdentityId']
    print "My ID: %s" % my_id
    try:
        response = anon_conn.get_open_id_token(IdentityId=my_id)
        if not 'Token' in response:
            print response
            return False
        assert(response['IdentityId'] == my_id)
        my_open_id_token = response['Token']

        sts_conn = anon_session.client('sts')
        session_name = 'anon-test-access'
        response = sts_conn.assume_role_with_web_identity(RoleArn=args.role_arn,
                                                          RoleSessionName=session_name,
                                                          WebIdentityToken=my_open_id_token,
                                                          )
        if not 'Credentials' in response:
            print response
            return False
        if response['SubjectFromWebIdentityToken'] != my_id:
            print "WARN: SubjectFromWebIdentityToken %s != my id %s" % (response['Audience'], my_id)
        my_creds = response
        del my_creds['ResponseMetadata']

        # {'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '841e102f-7f3e-11e4-9ab8-b7ff4d5c257d'},
        #  u'AssumedRoleUser': {u'AssumedRoleId': 'AROAIA2UHXJJLI5VA5ORY:foo',
        #                       u'Arn': 'arn:aws:sts::610813048224:assumed-role/Cognito_dev_telemetryUnauth_DefaultRole/foo'
        #                       },
        #  u'Audience': 'us-east-1:0d40f2cf-bf6c-4875-a917-38f8867b59ef',
        #  u'Provider': 'cognito-identity.amazonaws.com',
        #  u'SubjectFromWebIdentityToken': 'us-east-1:5507c03b-d395-4555-a32b-8f2e855b76ca',
        #  u'Credentials': {u'SecretAccessKey': 'aH5HOYykTTf4+WbgYYu8bU9VtwEl03jY0HEHM+kC',
        #                   u'SessionToken': 'AQoDYXdzECIa0AN8Vxb8D/Y8S/AfMJdCiNZdGqcvJwXGU6OhZPO/RWYMhOM9TBTa/b5n9v2lMthIxvGOCkn4erjk9XAwiuh2N7Yve2U4Q+jA3hCWbkg6O2Mlv4EYsUmtmpdsVpuLJdo4OgZCMfFQvAeOeItWWWIG9TYs7jYhxp3Ix4I0KrnE9tYdh33K61sKu0j81hKvP+3pKqMHsjzCwQYlAYb62TDro7b7Ca6sqM5wEuozNintzExQk1iZms7mzzKBOHOrj/X7DooqP2yX3CCGZJoxZf13r+vj56fRGIZ/AxaTXI4Ofau8RWY6y1/cmR/gXYpcdFdR3o6dlUt9Vg3MowSz2/B2YkEH4Gado2uHstZm6HJrUuWyVGgul5XHAW9Fq8Z7/Mo8JvqL1lfyFi7/wl1pzg8A5G5ytesMuNJmtZQhiX0RWCz3pC6aEPmGuN0V162YLw68ZingT53DHzuPAVpM7D1N7cC5b/F4h4uaeO47Ldykg4cOVm2P1IL0o8HIAlxh7+SUKIUNlbKBArtkouN2YrJ3dLvAWIBstAjPHonJamCWak0nGJZ4cp+CF+SQDFcF2O4qru07vVy8L312+MUtbZ0Vurl27OXeghyTc5L2e/qK61wP0SDAkpmkBQ==',
        #                   u'Expiration': datetime.datetime(2014, 12, 9, 1, 58, 40, tzinfo=tzutc()),
        #                   u'AccessKeyId': 'ASIAIESK3OGPFHPXHIOA'
        #                   }
        #  }
        new_session = boto3.session.Session(aws_access_key_id=my_creds['Credentials']['AccessKeyId'],
                                            aws_secret_access_key=my_creds['Credentials']['SecretAccessKey'],
                                            aws_session_token=my_creds['Credentials']['SessionToken'],
                                            region_name=args.region)
        s3_conn = new_session.client('s3')

        try:
            response = s3_conn.list_buckets()
        except ClientError as e:
            if not 'AccessDenied' in str(e):
                print e
                raise

        try:
            response = s3_conn.list_objects(Bucket=args.s3_bucket, MaxKeys=60)
        except ClientError as e:
            if not 'AccessDenied' in str(e):
                print e
                raise

        if args.s3_bucket_prefix:
            try:
                response = s3_conn.list_objects(Bucket=args.s3_bucket, MaxKeys=60, Prefix=args.s3_bucket_prefix)
            except ClientError as e:
                if not 'AccessDenied' in str(e):
                    print e
                    raise

        prefix = "/".join([args.s3_bucket_prefix, my_id])+"/"
        try:
            response = s3_conn.list_objects(Bucket=args.s3_bucket, MaxKeys=60, Prefix=prefix)
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                print response
                raise ClientError(response, 'ListBucket')
        except ClientError as e:
            print "Could not access what I should be able to access!: ListBucket s3://%s/%s" % (args.s3_bucket, prefix)
            print e
            return False

        # try:
        #     response = s3_conn.put_object(Bucket=args.s3_bucket, Key=prefix)
        #     if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        #         print response
        #         raise ClientError(response, 'PutObject')
        # except ClientError as e:
        #     print "Could not access what I should be able to access!: PutObject s3://%s/%s" % (args.s3_bucket, prefix)
        #     print e
        #     return False

        try:
            file = prefix + "somerandomfile.txt"
            response = s3_conn.put_object(Bucket=args.s3_bucket, Key=file, Body="foo12345\n")
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                print response
                raise ClientError(response, 'PutObject')
        except ClientError as e:
            print "Could not access what I should be able to access!: PutObject s3://%s/%s" % (args.s3_bucket, prefix)
            print e
            return False

    except Exception as e:
        print e

    finally:
        # clean up
        # don't bother cleaning up the identity, since it's an unauth'd id, and it can't be unlinked.
        pass
    return True



def main():
    parser = ArgumentParser()
    parser.add_argument('--secret-access-key', '-s',
                        help='AWS secret access key',
                        dest='aws_secret_access_key',
                        default=None)
    parser.add_argument('--access-key-id', '-a',
                        help='AWS access key id',
                        dest='aws_access_key_id',
                        default=None)
    parser.add_argument('--aws-account-id', help='AWS Account ID. A number.', default='610813048224')
    parser.add_argument('--region', help='Set the region for the connection. Default: us-east-1', default='us-east-1', type=str)
    parser.add_argument('--config', '-c',
                       help='Configuration that contains an AWS section',
                       default=None)
    parser.add_argument('-v', '--verbose', help='Verbose output', action='store_true', default=False)
    subparser = parser.add_subparsers()

    list_parser = subparser.add_parser('list-pools')
    list_parser.set_defaults(func=list_pools)
    list_parser.add_argument('--list-ids', help="List all ids. Could be a lot.", action='store_true', default=False)

    test_access_parser = subparser.add_parser('test-access')
    test_access_parser.set_defaults(func=test_access)
    test_access_parser.add_argument('--pool-id', help='A cognito pool id. use --list-pools to get one.',
                                    default='us-east-1:0d40f2cf-bf6c-4875-a917-38f8867b59ef')
    test_access_parser.add_argument('--role-arn', help='An IAM role ARN.',
                                    default='arn:aws:iam::610813048224:role/Cognito_dev_telemetryUnauth_DefaultRole')
    test_access_parser.add_argument('--s3-bucket', help='The nachomail accessible per-id bucket',
                                    default='460be238-6811-4527-90c2-82c46ee8a0f7')
    test_access_parser.add_argument('--s3-bucket-prefix', help='The nachomail accessible per-id bucket',
                                    default='NachoMail')

    args = parser.parse_args()

    if args.config:
        config_file = Config(args.config)
        AwsConfig(config_file).read(args)

    if not args.aws_access_key_id or not args.aws_secret_access_key:
        print "ERROR: No access-key or secret key. Need either a config or aws_access_key_id and aws_secret_access_key."
        sys.exit(1)

    if not args.func:
        raise ValueError('no function defined for argument')

    if args.verbose:
        print "Using AWS Key Id %s" % args.aws_access_key_id

    session = boto3.session.Session(aws_access_key_id=args.aws_access_key_id,
                                    aws_secret_access_key=args.aws_secret_access_key,
                                    region_name=args.region)

    conn = session.client('cognito-identity')
    ret = args.func(conn, args)
    if ret is None:
        sys.exit(0)

    if not ret:
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
