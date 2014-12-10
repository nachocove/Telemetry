# Copyright 2014, NachoCove, Inc
from argparse import ArgumentParser
import copy
import json
import sys
import boto3
from botocore.exceptions import ClientError
from AWS.config import AwsConfig
from misc.config import Config

class ArgFunc(object):
    class ResponseCheckException(Exception):
        pass

    def add_arguments(self, parser, subparser):
        pass

    def run(self, session, args, **kwargs):
        pass

    @classmethod
    def check_response(self, response, status_code=200, expected_keys=None):
        try:
            if response['ResponseMetadata']['HTTPStatusCode'] != status_code:
                msg=str(response)
                raise self.ResponseCheckException(msg)
        except KeyError:
            msg=str(response)
            raise self.ResponseCheckException(msg)

        if expected_keys:
            for k in expected_keys:
                if not k in response:
                    msg="Missing key %s in response" % k
                    raise self.ResponseCheckException(msg)
        del response['ResponseMetadata']
        return response

class DeletePools(ArgFunc):
    def add_arguments(self, parser, subparser):
        sub = subparser.add_parser('delete-pools')
        sub.add_argument('--name-prefix', help="delete all pools starting with the prefix.", type=str, default='')
        sub.add_argument('--pool-id', help="delete specific pool with id.", type=str, default='')
        return sub

    def run(self, session, args, **kwargs):
        if not args.name_prefix and not args.pool_id:
            print "ERROR: Need a name prefix or a pool-id."
        self.delete_pools(session, args.pool_id, args.name_prefix, args.verbose)
        self.delete_roles(session, args.name_prefix, SetupPool.role_name_path, args.verbose)
        return True

    @classmethod
    def delete_pools(cls, session, pool_id=None, name_prefix=None, verbose=False):
        conn = session.client('cognito-identity')
        pool_list = []
        if pool_id:
            response = conn.describe_identity_pool(IdentityPoolId=pool_id)
            pool = cls.check_response(response, 'IdentityPool')
            pool_list.append(pool)
        if name_prefix:
            response = conn.list_identity_pools(MaxResults=60)
            id_pools = cls.check_response(response, expected_keys=('IdentityPools',))
            for pool in id_pools['IdentityPools']:
                if pool['IdentityPoolName'].startswith(name_prefix):
                    pool_list.append(pool)
                    if verbose:
                        print "DELETE_POOLS: Found Pool %(IdentityPoolId)s with name %(IdentityPoolName)s" % pool
        for pool in pool_list:
            if verbose:
                print "DELETE_POOLS: id %s" % pool['IdentityPoolId']
            response = conn.delete_identity_pool(IdentityPoolId=pool['IdentityPoolId'])
            cls.check_response(response)
        if verbose:
            print "DELETE_POOLS: %d pools deleted" % len(pool_list)


    @classmethod
    def delete_roles(cls, session, role_name, role_path, verbose=False):
        iam = session.client('iam')
        response = iam.list_roles(PathPrefix=role_path)
        roles = cls.check_response(response, expected_keys=('Roles',))
        count = 0
        for role in roles['Roles']:
            if not role['RoleName'].startswith(role_name):
                continue
            policy_name = role['RoleName']+'Policy'
            print 'DELETE_ROLE_POLICY: %s: PolicyName=%s' % (role['RoleName'], policy_name)
            response = iam.delete_role_policy(RoleName=role['RoleName'], PolicyName=policy_name)
            cls.check_response(response)
            print 'DELETE_ROLES: %(RoleName)s: id=%(RoleId)s, Path=%(Path)s, Arn=%(Arn)s' % role
            response = iam.delete_role(RoleName=role['RoleName'])
            cls.check_response(response)
            count += 1
        if verbose:
            print "DELETE_ROLES: %d roles deleted" % count

class ListPools(ArgFunc):
    def add_arguments(self, parser, subparser):
        list_parser = subparser.add_parser('list-pools')
        list_parser.add_argument('--list-ids', help="List all ids. Could be a lot.",
                                 action='store_true', default=False)
        return list_parser

    def run(self, session, args, **kwargs):
        conn = session.client('cognito-identity')

        identity_pools = conn.list_identity_pools(MaxResults=60)
        for ip in identity_pools['IdentityPools']:
            pool = conn.describe_identity_pool(IdentityPoolId=ip['IdentityPoolId'])
            print "%(IdentityPoolName)s: id=%(IdentityPoolId)s " \
                  "AllowUnauthenticatedIdentities=%(AllowUnauthenticatedIdentities)s" % pool
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
        iam = session.client('iam')
        response = iam.list_roles()
        self.check_response(response, expected_keys=('Roles',))
        print "Roles:"
        for role in response['Roles']:
            print '%(RoleName)s: id=%(RoleId)s, Path=%(Path)s, Arn=%(Arn)s' % role
            if args.verbose:
                print "Policy=%s" % json.dumps(role[u'AssumeRolePolicyDocument']['Statement'], indent=4)
        return True


class SetupPool(ArgFunc):
    trust_dict = {
        "Action": "sts:AssumeRoleWithWebIdentity",
        "Principal": {
            "Federated": "cognito-identity.amazonaws.com"
        },
        "Effect": "Allow",
        "Condition": {
            "StringEquals": {
                "cognito-identity.amazonaws.com:aud": "%(IdentityPoolId)s"
            },
            "ForAnyValue:StringLike": {
                "cognito-identity.amazonaws.com:amr": "%(authenticated_or_not)s"
            }
        },
    }

    policy_dict_statement_template = {"Action": [],
                                      "Effect": "Allow",
                                      "Resource": [],
    }
    policy_dict_template = {
        "Version": "2012-10-17",
        "Statement": []
    }
    dynamo_table_ops = ["dynamodb:PutItem",
                        "dynamodb:DescribeTable",
                        "dynamodb:BatchWriteItem"]
    dynamo_arn_table_template = "arn:aws:dynamodb:%(RegionName)s:%(AccountId)s:table/%(DynamoTableName)s"

    s3_ops = ["s3:GetObject",
              "s3:GetObjectVersion",
              "s3:DeleteObject",
              "s3:DeleteObjectVersion",
              "s3:ListBucket",
              "s3:ListBucketVersions",
              "s3:RestoreObject"]

    s3_bucket_path_template = ["arn:aws:s3:::%(BucketName)s/%(PathPrefix)s${cognito-identity.amazonaws.com:sub}/*",
                               "arn:aws:s3:::%(BucketName)s/%(PathPrefix)s${cognito-identity.amazonaws.com:sub}*"]

    role_name_path = '/nachomail/cognito/'
    role_name_template = '%(Pool_Name)s_%(Auth_or_Unauth)s_DefaultRole'

    default_ops_bucket_name = '460be238-6811-4527-90c2-82c46ee8a0f7'
    default_bucket_prefix = "NachoMail/"
    default_dynamo_tables = ["%(project)s.telemetry.device_info",
                             "%(project)s.telemetry.log",
                             "%(project)s.telemetry.wbxml",
                             "%(project)s.telemetry.capture",
                             "%(project)s.telemetry.counter",
                             "%(project)s.telemetry.support",
                             "%(project)s.telemetry.ui", ]

    def add_arguments(self, parser, subparser):
        sub = subparser.add_parser('setup-pool')
        sub.add_argument('name', help="the identity pool name", type=str)
        sub.add_argument('--no-unauth', help='Do not allow unauth access.', action='store_true', default=False)
        sub.add_argument('--developer-provider-name',
                         help='The DeveloperProviderName, if we do our own authenticating.', type=str)
        sub.add_argument('--s3-bucket', help='The nachomail accessible per-id bucket',
                                        default=self.default_ops_bucket_name)
        sub.add_argument('--s3-bucket-prefix', help='The nachomail accessible per-id bucket',
                                        default=self.default_bucket_prefix)
        sub.add_argument('--aws_prefix', help='The aws project prefix', default='dev')
        return sub

    def run(self, session, args, **kwargs):
        unauth_policy_supported = False if args.no_unauth else True
        pool = self.create_identity_pool(session, args.name,
                                         developer_provider_name=args.developer_provider_name,
                                         unauth_policy_supported=unauth_policy_supported, verbose=args.verbose)
        self.create_identity_roles_and_policy(session, pool, args.aws_account_id,
                                              self.role_name_path,
                                              [x % {'project': args.aws_prefix} for x in self.default_dynamo_tables],
                                              args.s3_bucket,
                                              args.s3_bucket_prefix,
                                              developer_provider_name=None,
                                              unauth_policy_supported=True,
                                              dynamo_table_op_perms=None,
                                              s3_op_perms=None,
                                              verbose=args.verbose)

    @classmethod
    def create_cognito_role(cls, pool, iam_client, role_path, actions, resources, auth=False, verbose=False):
        auth_role = {}
        auth_role['RoleName'] = cls.role_name_template % {'Auth_or_Unauth': 'Auth' if auth else 'UnAuth',
                                                           'Pool_Name': pool['IdentityPoolName']
        }

        # convert to json, do replacement, and then convert back to dict, since the create_role call wants a dict.
        policy_json = json.dumps({"Version":"2012-10-17","Statement":[cls.trust_dict]},
                                 indent=4) % {'authenticated_or_not': 'authenticated' if auth else 'unauthenticated',
                                              'IdentityPoolId': pool['IdentityPoolId'],
                                              }
        auth_role['AssumeRolePolicyDocument'] = policy_json

        if role_path:
            auth_role['Path'] = role_path
        try:
            response = iam_client.create_role(**auth_role)
        except ClientError as e:
            print auth_role
            print e
            raise
        role = cls.check_response(response, expected_keys=('Role',))
        role = role['Role']
        if verbose:
            print "CREATE_ROLE: %(RoleName)s: id=%(RoleId)s Path=%(Path)s" % role

        auth_role_policy = copy.deepcopy(cls.policy_dict_template)
        statement = copy.deepcopy(cls.policy_dict_statement_template)
        for action in actions:
            statement['Action'].append(action)
        for resource in resources:
            statement['Resource'].append(resource)
        auth_role_policy['Statement'].append(statement)

        policy_name = role['RoleName']+'Policy'
        if verbose:
            print "CREATE_ROLE_POLICY: %s: PolicyName=%s" % (role['RoleName'], policy_name)
        response = iam_client.put_role_policy(RoleName=role['RoleName'],
                                              PolicyName=policy_name,
                                              PolicyDocument=json.dumps(auth_role_policy))
        cls.check_response(response)
        return role

    @classmethod
    def create_identity_pool(cls, session, name, developer_provider_name=None, unauth_policy_supported=True, verbose=False):
        conn = session.client('cognito-identity')

        create_kwargs = {'IdentityPoolName': name,
                         'AllowUnauthenticatedIdentities': unauth_policy_supported,
                         'SupportedLoginProviders': {},
        }
        if developer_provider_name:
            create_kwargs['DeveloperProviderName'] = developer_provider_name
        if verbose:
            print "CREATE_IDENTITY_POOL: %s" % create_kwargs
        response = conn.create_identity_pool(**create_kwargs)
        pool = cls.check_response(response, expected_keys=('IdentityPoolId', 'IdentityPoolName'))
        if verbose:
            print "CREATE_IDENTITY_POOL: %(IdentityPoolId)s: Name=%(IdentityPoolName)s" % pool
        return pool

    @classmethod
    def create_identity_roles_and_policy(cls, session, pool, aws_account_id,
                                         role_path,
                                         dynamo_tables_names,
                                         bucket_name,
                                         bucket_path_prefix,
                                         developer_provider_name=None,
                                         unauth_policy_supported=True,
                                         dynamo_table_op_perms=None,
                                         s3_op_perms=None,
                                         verbose=False):
        if dynamo_table_op_perms is None:
            dynamo_table_op_perms = cls.dynamo_table_ops
        if s3_op_perms is None:
            s3_op_perms = cls.s3_ops

        iam = session.client('iam')
        roles_created = []
        if developer_provider_name:
            resources = []
            for table in dynamo_tables_names:
                resources.append(cls.dynamo_arn_table_template % {'RegionName': 'us-west-2',
                                                                   'AccountId': aws_account_id,
                                                                   'DynamoTableName': table})
            for s3 in cls.s3_bucket_path_template:
                resources.append(s3 % {'BucketName': bucket_name,
                                       'PathPrefix': bucket_path_prefix})

            roles_created.append(cls.create_cognito_role(pool, iam, cls.role_name_path,
                                                         cls.dynamo_table_ops + cls.s3_ops,
                                                         resources, auth=True, verbose=verbose))

        if unauth_policy_supported:
            resources = []
            for table in dynamo_tables_names:
                resources.append(cls.dynamo_arn_table_template % {'RegionName': 'us-west-2',
                                                                   'AccountId': aws_account_id,
                                                                   'DynamoTableName': table})
            for s3 in cls.s3_bucket_path_template:
                resources.append(s3 % {'BucketName': bucket_name,
                                       'PathPrefix': bucket_path_prefix})

            roles_created.append(cls.create_cognito_role(pool, iam, role_path,
                                                         dynamo_table_op_perms + s3_op_perms,
                                                         resources, auth=False, verbose=verbose))
        if not roles_created:
            print "ERROR: No roles created"
            return False
        return roles_created



class TestAccess(ArgFunc):
    def add_arguments(self, parser, subparser):
        sub = subparser.add_parser('test-access')
        sub.add_argument('--pool-id', help='A cognito pool id. use --list-pools to get one.',
                                        default='us-east-1:0d40f2cf-bf6c-4875-a917-38f8867b59ef')
        sub.add_argument('--s3-bucket', help='The nachomail accessible per-id bucket',
                                        default='460be238-6811-4527-90c2-82c46ee8a0f7')
        sub.add_argument('--s3-bucket-prefix', help='The nachomail accessible per-id bucket',
                                        default='NachoMail')
        return sub

    def run(self, session, args, **kwargs):
        conn = session.client('cognito-identity')

        response = conn.describe_identity_pool(IdentityPoolId=args.pool_id)
        pool = self.check_response(response, expected_keys=('IdentityPoolId', 'IdentityPoolName'))


        anon_session = boto3.session.Session(aws_access_key_id='', aws_secret_access_key='', region_name=args.region)
        anon_session._session.set_credentials(access_key='', secret_key='')
        anon_conn = anon_session.client('cognito-identity')
        response = anon_conn.get_id(AccountId=args.aws_account_id, IdentityPoolId=args.pool_id)
        self.check_response(response, expected_keys=('IdentityId',))
        my_id = response['IdentityId']
        print "My ID: %s" % my_id
        try:
            response = anon_conn.get_open_id_token(IdentityId=my_id)
            self.check_response(response, expected_keys=('Token',))
            assert(response['IdentityId'] == my_id)
            my_open_id_token = response['Token']

            sts_conn = anon_session.client('sts')
            session_name = 'anon-test-access'
            response = sts_conn.assume_role_with_web_identity(RoleArn=args.role_arn,
                                                              RoleSessionName=session_name,
                                                              WebIdentityToken=my_open_id_token,
                                                              )
            self.check_response(response, expected_keys=('Credentials',))
            if response['SubjectFromWebIdentityToken'] != my_id:
                print "WARN: SubjectFromWebIdentityToken %s != my id %s" % (response['Audience'], my_id)
            my_creds = response
            del my_creds['ResponseMetadata']

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

            bad_prefix = "/".join([args.s3_bucket_prefix, my_id])+"1233456"
            try:
                response = s3_conn.put_object(Bucket=args.s3_bucket, Key=bad_prefix)
                self.check_response(response)
            except ClientError as e:
                    if not 'AccessDenied' in str(e):
                        print e
                        raise

            prefix = "/".join([args.s3_bucket_prefix, my_id])+"/"
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
                self.check_response(response)
            except ClientError as e:
                print "Could not access what I should be able to access!: PutObject s3://%s/%s" % (args.s3_bucket, prefix)
                print e
                return False

            try:
                response = s3_conn.list_objects(Bucket=args.s3_bucket, MaxKeys=60, Prefix=prefix)
                self.check_response(response)
            except ClientError as e:
                print "Could not access what I should be able to access!: ListBucket s3://%s/%s" % (args.s3_bucket, prefix)
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
    sub_modules = (ListPools(),
                   TestAccess(),
                   SetupPool(),
                   DeletePools())

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

    for sub in sub_modules:
        s = sub.add_arguments(parser, subparser)
        s.set_defaults(func=sub.run)


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

    ret = args.func(session, args)
    if ret is None:
        sys.exit(0)

    if not ret:
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
