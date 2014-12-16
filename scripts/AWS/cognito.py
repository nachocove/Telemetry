# Copyright 2014, NachoCove, Inc
"""
Cognito Setup Program
=====================

This program is used to setup, list, test, and delete cognito setups.

A cognito setup consists of the following items:

* A cognito identity-pool with a name we pick, with authentication settings, most of which we
  don't use (google openid, etc..), except for setting (or not) the flag that allows
  unauthenticated access (default is True for us).

* One or more IAM Roles that are linked to the Identity Pool, each of which have a policy applied
  that determines what the cognito user may or may not do.

* A set of DynamoDB tables the user can write to (but not read). The permissions to the tables can be found in
  SetupPool.dynamo_table_permissions

* An AWS S3 bucket and subdirectory, where cognito users may read or write anything they want.
  Note that each user is constrained to a sub-directory matching their cognito-id. They may
  NOT see or list any other objects.

  The S3 bucket-name is a uuid4 hex-string (because I see no need to name it anything that gives away
  what it's for or who it belongs to), and the NachoMail add is restricted to the NachoMail subdirectory (i.e. prefix).
  Each client is restricted to the NachoMail/<cognito-id>/ sub-directory (i.e. prefix). The details of the policy
  can be seen in SetupPool.s3_object_permissions and SetupPool.s3_bucket_permissions.

setup-pool
----------

Using the option 'setup-pool', we create:

* The Cognito Identity Pool
* The IAM Roles and policies

We also check that the S3 bucket exists, and that it has versioning turned on (we turn it on, if it doesn't,
but we do NOT create the bucket).

We do not do any checking of the DynamoDB (Should be added, i think).

Using the Identity Pool Id, we create any number of IAM Roles, each of which contains
a linkage policy that defines which role is applied to a cognito user under which conditions,
and a policy that gives the user permissions to access whatever AWS resources we want.

.. note:: aws allows us to name roles in a subdirectory structure. I've opted to put the nachomail roles
  under SetupPool.role_name_path (currently set to /nachomail/cognito/). This makes it easier to list all
  nachomail cognito roles: Just list with prefix '/nachomail/cognito/'.

In our case we create two roles (though technically one would suffice since the role-policy
is the same in all cases (for now)): One for authenticated users, and one for unauthenticated users.

.. note:: These roles do not show up in the cognito console! The only way to determine the exist (and
  apply to the cognito identity pool) is to go to the IAM console -> Roles and check the 'Trust Relationships'
  setting for each role. The Trust Entity will be 'cognito-identity.amazonaws.com', and the Conditions make
  mention of cognito-identity.amazonaws.com:aud, which must match the Identity Pool's ID. An additional condition
  specifies whether this role pertains to authenticated or unauthenticated users.

.. note:: AWS does not restrict the creation of multiple identity pools with the same name. This script,
    however, does. We first check that the pool does not already exist. If it does, we print the fact and exit.

list-pools
----------

This lists all the identity pools and the policies and roles pertaining to it.

delete-pools
------------

Deletes all pools that start with the given string. In practice there will usually only be one (See note about
identity pool naming in the 'setup-pool' section).

test-access
-----------

Used to test access to resources that a client should have access to, as well as test to make sure the client
does NOT have access to resources it shouldn't have access to.

.. note:: This creates an ID and some files in S3, which we leave behind (partly to test the delete-pools command).


"""
from argparse import ArgumentParser
import copy
from datetime import datetime
import json
import logging
import sys
import uuid
import boto3
from botocore.exceptions import ClientError
from AWS.config import AwsConfig, CliFunc
from AWS.tables import TelemetryTable, LogTable
from misc.config import Config
from unittest import TextTestRunner, TestCase, TestLoader
from misc.utc_datetime import UtcDateTime

logger = logging.getLogger('cognito-setup')

class Boto3CliFunc(CliFunc):
    def run(self, args, **kwargs):
        super(Boto3CliFunc, self).run(args, **kwargs)
        self.session = boto3.session.Session(aws_access_key_id=args.aws_access_key_id,
                                             aws_secret_access_key=args.aws_secret_access_key,
                                             region_name=args.region)
        if args.boto_debug:
            self.session._session.set_debug_logger()

    class ResponseCheckException(Exception):
        pass

    @classmethod
    def check_response(cls, response, status_code=200, expected_keys=None):
        try:
            if response['ResponseMetadata']['HTTPStatusCode'] != status_code:
                msg=str(response)
                raise cls.ResponseCheckException(msg)
        except KeyError:
            msg=str(response)
            raise cls.ResponseCheckException(msg)

        if expected_keys:
            for k in expected_keys:
                if not k in response:
                    msg="Missing key %s in response" % k
                    raise cls.ResponseCheckException(msg)
        del response['ResponseMetadata']
        return response


class DeletePools(Boto3CliFunc):
    def add_arguments(self, parser, subparser):
        super(DeletePools, self).add_arguments(parser, subparser)
        sub = subparser.add_parser('delete-pools')
        sub.add_argument('--name-prefix', help="delete all pools starting with the prefix.", type=str, default='')
        sub.add_argument('--pool-id', help="delete specific pool with id.", type=str, default='')
        sub.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(DeletePools, self).run(args, **kwargs)
        if not args.name_prefix and not args.pool_id:
            logger.error("Need a name prefix or a pool-id.")
        self.delete_pools(self.session, args.pool_id, name_prefix=args.name_prefix, s3_bucket=args.aws_s3_bucket)
        self.delete_roles(self.session, args.name_prefix, SetupPool.role_name_path)
        return True

    @classmethod
    def delete_s3_objects(cls, bucket, prefix):
        delete_list = {'Quiet': True,
                       'Objects': []}
        if bucket.BucketVersioning().status == 'Enabled':
            versions = bucket.object_versions.filter(Prefix=prefix)
            for version in versions:
                delete_list['Objects'].append({'Key': version.object_key, 'VersionId': version.id})
        objects = bucket.objects.filter(Prefix=prefix)
        for object in objects:
            delete_list['Objects'].append({'Key': object.key})
        if delete_list['Objects']:
            response = bucket.delete_objects(Delete=delete_list)
            cls.check_response(response)

    @classmethod
    def delete_s3_user_resources(cls, session, pool_id, s3_bucket, bucket_prefix):
        conn = session.client('cognito-identity')
        s3 = session.resource('s3')
        bucket = s3.Bucket(s3_bucket)
        next_token = None
        while True:
            list_kwargs = {'IdentityPoolId': pool_id, 'MaxResults': 60}
            if next_token:
                list_kwargs['NextToken'] = next_token
            response = conn.list_identities(**list_kwargs)
            cls.check_response(response)
            next_token = response.get('Marker', None)
            identity_list = response['Identities']
            for cognito_id in identity_list:
                prefix = "/".join([bucket_prefix, cognito_id['IdentityId'], ''])
                logger.info("DELETE_S3_FILES: cognito-id: %s, s3://%s/%s", cognito_id, s3_bucket, prefix)
                cls.delete_s3_objects(bucket, bucket_prefix)
            if not next_token:
                break

    @classmethod
    def delete_pools(cls, session, pool_id, name_prefix, s3_bucket):
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
                    logger.info("DELETE_POOLS: Found Pool %(IdentityPoolId)s with name %(IdentityPoolName)s", pool)
        for pool in pool_list:
            if s3_bucket:
                cls.delete_s3_user_resources(session, pool['IdentityPoolId'], s3_bucket,
                                             SetupPool.default_bucket_prefix)
            logger.info("DELETE_POOLS: id %s", pool['IdentityPoolId'])
            response = conn.delete_identity_pool(IdentityPoolId=pool['IdentityPoolId'])
            cls.check_response(response)
        logger.info("DELETE_POOLS: %d pools deleted", len(pool_list))


    @classmethod
    def delete_roles(cls, session, role_name, role_path):
        iam = session.client('iam')
        response = iam.list_roles(PathPrefix=role_path)
        roles = cls.check_response(response, expected_keys=('Roles',))
        count = 0
        for role in roles['Roles']:
            if not role['RoleName'].startswith(role_name):
                continue
            policy_name = role['RoleName']+'Policy'
            logger.info('DELETE_ROLE_POLICY: %s: PolicyName=%s', role['RoleName'], policy_name)
            response = iam.delete_role_policy(RoleName=role['RoleName'], PolicyName=policy_name)
            cls.check_response(response)
            logger.info('DELETE_ROLES: %(RoleName)s: id=%(RoleId)s, Path=%(Path)s, Arn=%(Arn)s', role)
            response = iam.delete_role(RoleName=role['RoleName'])
            cls.check_response(response)
            count += 1
        logger.info("DELETE_ROLES: %d roles deleted", count)

class ListPools(Boto3CliFunc):
    def add_arguments(self, parser, subparser):
        list_parser = subparser.add_parser('list-pools')
        list_parser.add_argument('--list-ids', help="List all ids. Could be a lot.",
                                 action='store_true', default=False)
        list_parser.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(ListPools, self).run(args, **kwargs)
        conn = self.session.client('cognito-identity')
        logger.setLevel(logging.INFO)

        identity_pools = conn.list_identity_pools(MaxResults=60)
        for ip in identity_pools['IdentityPools']:
            pool = conn.describe_identity_pool(IdentityPoolId=ip['IdentityPoolId'])
            logger.info("%(IdentityPoolName)s: id=%(IdentityPoolId)s "
                        "AllowUnauthenticatedIdentities=%(AllowUnauthenticatedIdentities)s", pool)
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
                    logger.info(",".join(id_list))
                    next_token = ids['NextToken']
        iam = self.session.client('iam')
        response = iam.list_roles()
        self.check_response(response, expected_keys=('Roles',))
        logger.info("Roles:")
        for role in response['Roles']:
            logger.info('%(RoleName)s: id=%(RoleId)s, Path=%(Path)s, Arn=%(Arn)s', role)
            logger.debug("Policy=%s", json.dumps(role[u'AssumeRolePolicyDocument']['Statement'], indent=4))
        return True


class SetupPool(Boto3CliFunc):
    """
    Sets up everything needed for a NachoMail Cognito setup.

    - Cognito Identity pool with name given on command line
    - If unauthenticated is allowed (default=True), create an IAM Role and attach a Trust-policy for it
      - Also create a policy for dynamoDb and S3 for the unauthenticated access
    - If authenticated is allowed (default=True), create an IAM Role and attach a Trust-policy for it
      - Also create a policy for dynamoDb and S3 for the authenticated access (identical to unauth, currently)

    """
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

    policy_dict_template = {
        "Version": "2012-10-17",
        "Statement": []
    }
    dynamo_table_permissions = {'Action': ["dynamodb:PutItem",
                                           "dynamodb:DescribeTable",
                                           "dynamodb:BatchWriteItem"],
                                'Resource': [],
                                'Effect': 'Allow'
                                }
    dynamo_arn_table_template = "arn:aws:dynamodb:%(RegionName)s:%(AccountId)s:table/%(DynamoTableName)s"
    dynamo_region = 'us-west-2'

    s3_object_permissions = {'Action': ["s3:PutObject",
                                        "s3:GetObject",
                                        "s3:GetObjectVersion",
                                        "s3:DeleteObject",
                                        "s3:DeleteObjectVersion",
                                        "s3:RestoreObject"],
                             "Effect": "Allow",
                             "Resource": ["arn:aws:s3:::%(BucketName)s/%(PathPrefix)s${cognito-identity.amazonaws.com:sub}/*",],
                             }
    s3_bucket_permissions = {'Action': ["s3:ListBucket",
                                        "s3:ListBucketVersions",
                                        ],
                             'Resource': ["arn:aws:s3:::%(BucketName)s",],
                             'Effect': 'Allow',
                             "Condition": {"StringLike": {"s3:prefix": ["%(PathPrefix)s${cognito-identity.amazonaws.com:sub}/*"]}}
                             }

    role_name_path = '/nachomail/cognito/'
    role_name_template = '%(Pool_Name)s_%(Auth_or_Unauth)s_DefaultRole'

    default_bucket_prefix = "NachoMail"

    # TODO Perhaps we should read these from dynamoDB instead of assuming?
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
        sub.add_argument('--no-auth', help='Disallow authenticated access.', action='store_true', default=False)
        sub.add_argument('--developer-provider-name',
                         help='The DeveloperProviderName, if we do our own authenticating.', type=str)
        sub.add_argument('--s3-bucket', help='The nachomail accessible per-id bucket',
                                        default=None)
        sub.add_argument('--s3-bucket-prefix', help='The nachomail accessible per-id bucket',
                                        default=self.default_bucket_prefix)
        sub.add_argument('--aws_prefix', help='The aws project prefix', default='dev')
        sub.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(SetupPool, self).run(args, **kwargs)
        unauth_policy_supported = False if args.no_unauth else True
        auth_policy_supported = False if args.no_auth else True
        return self.setup_cognito(self.session, args.name, unauth_policy_supported, auth_policy_supported,
                                  args.developer_provider_name, args.aws_s3_bucket, args.s3_bucket_prefix,
                                  args.aws_account_id, args.aws_prefix)

    def setup_cognito(self, session, name, unauth_policy_supported, auth_policy_supported, developer_provider_name,
                      aws_s3_bucket, s3_bucket_prefix, aws_account_id, aws_prefix):
        pool = self.create_identity_pool(session, name,
                                         developer_provider_name=developer_provider_name,
                                         unauth_policy_supported=unauth_policy_supported)
        if not pool:
            logger.error("Could not create pool.")
            return False
        self.create_or_adjust_s3_bucket(session, aws_s3_bucket)
        self.create_identity_roles_and_policy(session, pool, aws_account_id,
                                              self.role_name_path,
                                              [x % {'project': aws_prefix} for x in self.default_dynamo_tables],
                                              aws_s3_bucket,
                                              s3_bucket_prefix,
                                              unauth_policy_supported=unauth_policy_supported,
                                              auth_policy_supported=auth_policy_supported)
        return True


    @classmethod
    def create_or_adjust_s3_bucket(cls, session, bucket_name, path_prefix=None):
        if path_prefix is None:
            path_prefix = cls.default_bucket_prefix
        s3_conn = session.client('s3')
        response = s3_conn.list_buckets()
        cls.check_response(response, expected_keys=('Buckets',))
        found = False
        for bucket in response['Buckets']:
            if bucket['Name'] == bucket_name:
                found = True
                break
        s3 = session.resource('s3')
        bucket = s3.Bucket(bucket_name)
        if not found:
            bucket.create()
        versioning = bucket.BucketVersioning()
        if not versioning.status == 'Enabled':
            response = versioning.enable()
            cls.check_response(response)
        if path_prefix:
            prefix = bucket.Object(path_prefix+'/')
            try:
                prefix.get()
            except ClientError as e:
                if not 'NoSuchKey' in str(e):
                    raise
                response = prefix.put()
                cls.check_response(response)


    @classmethod
    def create_cognito_role(cls, pool, iam_client, role_path, statements, auth=False):
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
            import traceback
            logger.error(auth_role)
            logger.error("%s\n%s", e, traceback.format_exc())
            raise
        role = cls.check_response(response, expected_keys=('Role',))
        role = role['Role']
        logger.info("CREATE_ROLE: %(RoleName)s: id=%(RoleId)s Path=%(Path)s", role)

        auth_role_policy = copy.deepcopy(cls.policy_dict_template)
        auth_role_policy['Statement'] = statements

        policy_name = role['RoleName']+'Policy'
        logger.info("CREATE_ROLE_POLICY: %s: PolicyName=%s", role['RoleName'], policy_name)
        response = iam_client.put_role_policy(RoleName=role['RoleName'],
                                              PolicyName=policy_name,
                                              PolicyDocument=json.dumps(auth_role_policy, indent=4))
        cls.check_response(response)
        return role

    @classmethod
    def create_identity_pool(cls, session, name, developer_provider_name=None, unauth_policy_supported=True):
        conn = session.client('cognito-identity')

        response = conn.list_identity_pools(MaxResults=60)
        pools = cls.check_response(response, expected_keys=('IdentityPools',))
        for p in pools['IdentityPools']:
            if p['IdentityPoolName'] == name:
                logger.error("Identity Pool with name %s already exists. Pick a new name or delete the pool first.",
                             name)
                return False

        create_kwargs = {'IdentityPoolName': name,
                         'AllowUnauthenticatedIdentities': unauth_policy_supported,
                         'SupportedLoginProviders': {},
        }
        if developer_provider_name:
            create_kwargs['DeveloperProviderName'] = developer_provider_name
        logger.info("CREATE_IDENTITY_POOL: %s", create_kwargs)
        response = conn.create_identity_pool(**create_kwargs)
        pool = cls.check_response(response, expected_keys=('IdentityPoolId', 'IdentityPoolName'))
        logger.info("CREATE_IDENTITY_POOL: %(IdentityPoolId)s: Name=%(IdentityPoolName)s", pool)
        return pool

    @classmethod
    def create_identity_roles_and_policy(cls, session, pool, aws_account_id,
                                         role_path,
                                         dynamo_tables_names,
                                         bucket_name,
                                         bucket_path_prefix,
                                         unauth_policy_supported=True,
                                         auth_policy_supported=False,
                                         dynamo_table_permissions=None,
                                         s3_object_permissions=None,
                                         s3_bucket_permissions=None):
        if dynamo_table_permissions is None:
            dynamo_table_permissions = cls.dynamo_table_permissions
        if s3_object_permissions is None:
            s3_object_permissions = cls.s3_object_permissions
        if s3_bucket_permissions is None:
            s3_bucket_permissions = cls.s3_bucket_permissions

        if not bucket_path_prefix.endswith('/'):
            bucket_path_prefix += '/'

        iam = session.client('iam')
        roles_created = []

        statements = cls.munge_statements(aws_account_id, cls.dynamo_region,
                                          dynamo_tables_names, dynamo_table_permissions,
                                          bucket_name, bucket_path_prefix,
                                          s3_object_permissions, s3_bucket_permissions)

        if auth_policy_supported:
            roles_created.append(cls.create_cognito_role(pool, iam, cls.role_name_path, statements, auth=True))

        if unauth_policy_supported:
            roles_created.append(cls.create_cognito_role(pool, iam, cls.role_name_path, statements, auth=False))

        if not roles_created:
            logger.error("No roles created")
            return False
        return roles_created

    @classmethod
    def munge_statements(cls, aws_account_id, aws_dynamo_region,
                         dynamo_tables_names,
                         dynamo_table_permissions,
                         bucket_name, bucket_path_prefix,
                         s3_object_permissions,
                         s3_bucket_permissions):
        statements = []
        for table in dynamo_tables_names:
            dynamo_table_permissions['Resource'].append(cls.dynamo_arn_table_template % {'RegionName': aws_dynamo_region,
                                                                                         'AccountId': aws_account_id,
                                                                                         'DynamoTableName': table})
        statements.append(dynamo_table_permissions)

        statements.append(json.loads(json.dumps(s3_object_permissions) % {'BucketName': bucket_name,
                                                                          'PathPrefix': bucket_path_prefix}))
        statements.append(json.loads(json.dumps(s3_bucket_permissions) % {'BucketName': bucket_name,
                                                                          'PathPrefix': bucket_path_prefix}))
        return statements

class TestAccess(Boto3CliFunc):
    def add_arguments(self, parser, subparser):
        sub = subparser.add_parser('test-pool')
        sub.add_argument('name', help='the identity pool name')
        sub.add_argument('--s3-bucket', help='The nachomail accessible per-id bucket',
                                        default=None)
        sub.add_argument('--s3-bucket-prefix', help='The nachomail accessible per-id bucket',
                                        default=SetupPool.default_bucket_prefix)
        sub.set_defaults(func=self.run)

    def run(self, args, **kwargs):
        super(TestAccess, self).run(args, **kwargs)
        conn = self.session.client('cognito-identity')
        response = conn.list_identity_pools(MaxResults=60)
        id_pools = self.check_response(response, expected_keys=('IdentityPools',))
        pool = None
        for p in id_pools['IdentityPools']:
            if p['IdentityPoolName'] == args.name:
                pool = p
                break
        if not pool:
            logger.error("Could not find identity pool with name %s", args.name)
            return False

        iam = self.session.client('iam')

        response = iam.list_roles(PathPrefix=SetupPool.role_name_path)
        roles = self.check_response(response, expected_keys=('Roles',))
        role = None
        look_for = pool['IdentityPoolName'] + "_UnAuth"
        for r in roles['Roles']:
            if r['RoleName'].startswith(look_for):
                role = r
                break
        if not role:
            logger.error("Could not find role for Identity Pool")
            return False

        class TestCases(TestCase):
            test_bucket = None
            dynamo_created_items = None
            dynamodb_root = None
            nacho_bucket = None
            prefix = None

            @classmethod
            def setUpClass(cls):
                s3 = self.session.resource('s3')
                cls.nacho_bucket = s3.Bucket(args.aws_s3_bucket)
                cls.test_bucket = s3.Bucket('SomeTestBucket' + uuid.uuid4().hex)
                cls.test_bucket.create()
                dynamo_session = boto3.session.Session(aws_access_key_id=args.aws_access_key_id,
                                                        aws_secret_access_key=args.aws_secret_access_key,
                                                        region_name='us-west-2')

                cls.dynamodb_root = dynamo_session.client('dynamodb')


                anon_session = boto3.session.Session(aws_access_key_id='', aws_secret_access_key='', region_name=args.region)
                anon_session._session.set_credentials(access_key='', secret_key='')
                anon_conn = anon_session.client('cognito-identity')
                response = anon_conn.get_id(AccountId=args.aws_account_id, IdentityPoolId=pool['IdentityPoolId'])
                TestAccess.check_response(response, expected_keys=('IdentityId',))
                cls.my_id = response['IdentityId']
                logger.info("Got Cognito ID: %s", cls.my_id)
                response = anon_conn.get_open_id_token(IdentityId=cls.my_id)
                TestAccess.check_response(response, expected_keys=('Token',))
                assert(response['IdentityId'] == cls.my_id)
                my_open_id_token = response['Token']

                sts_conn = anon_session.client('sts')
                session_name = 'anon-test-access'
                response = sts_conn.assume_role_with_web_identity(RoleArn=role['Arn'],
                                                                  RoleSessionName=session_name,
                                                                  WebIdentityToken=my_open_id_token,
                                                                  )
                TestAccess.check_response(response, expected_keys=('Credentials',))
                if response['SubjectFromWebIdentityToken'] != cls.my_id:
                    logger.warn("SubjectFromWebIdentityToken %s != my id %s", response['Audience'], cls.my_id)
                cls.my_creds = response

                cls.new_session = boto3.session.Session(aws_access_key_id=cls.my_creds['Credentials']['AccessKeyId'],
                                                        aws_secret_access_key=cls.my_creds['Credentials']['SecretAccessKey'],
                                                        aws_session_token=cls.my_creds['Credentials']['SessionToken'],
                                                        region_name=args.region)
                cls.s3_conn = cls.new_session.client('s3')

                dynamo_session = boto3.session.Session(aws_access_key_id=cls.my_creds['Credentials']['AccessKeyId'],
                                                    aws_secret_access_key=cls.my_creds['Credentials']['SecretAccessKey'],
                                                    aws_session_token=cls.my_creds['Credentials']['SessionToken'],
                                                    region_name='us-west-2')
                cls.dynamodb = dynamo_session.client('dynamodb')
                cls.prefix = "/".join([args.s3_bucket_prefix, cls.my_id, ''])
                TelemetryTable.PREFIX = args.aws_prefix
                cls.dynamo_created_items = []


            @classmethod
            def tearDownClass(cls):
                # clean up
                # don't bother cleaning up the identity, since it's an unauth'd id, and it can't be unlinked.
                logger.info('Cleaning up S3 items created')
                if cls.test_bucket:
                    cls.test_bucket.delete()

                DeletePools.delete_s3_objects(cls.nacho_bucket, cls.prefix)

                logger.info('Cleaning up DynamoDB items created')
                for x in cls.dynamo_created_items:
                    cls.dynamodb_root.delete_item(TableName=x['TableName'], Key={'id': x['Item']['id']})
                # query = Query()
                # query.add('event_type', SelectorEqual('DEBUG'))
                # query.add_range('uploaded_at', self.start, self.end)
                #
                # self.events, self.event_count = self.query_all(query)



            @classmethod
            def check_response(cls, response, status_code=200, expected_keys=None):
                return TestAccess.check_response(response, status_code=status_code, expected_keys=expected_keys)

            def raisesClientError(self, error, func, *args, **kwargs):
                try:
                    func(*args, **kwargs)
                    raise Exception('Should not have succeeded')
                except ClientError as e:
                    self.assertIn(error, str(e))

            def test_bucket_listing_denied(self):
                self.assertTrue(args.s3_bucket_prefix)  # if this isn't set, we might rethink some tests
                self.raisesClientError('AccessDenied', self.s3_conn.list_buckets)
                self.raisesClientError('AccessDenied', self.s3_conn.list_objects, Bucket=args.aws_s3_bucket, MaxKeys=60)
                self.raisesClientError('AccessDenied', self.s3_conn.list_objects, Bucket=self.test_bucket.name, MaxKeys=60)
                self.raisesClientError('AccessDenied', self.s3_conn.list_objects, Bucket=args.aws_s3_bucket, MaxKeys=60, Prefix=args.s3_bucket_prefix)

            def test_put_object_denied(self):
                bad_prefix = "/".join([args.s3_bucket_prefix, self.my_id])+"1233456"
                self.raisesClientError('AccessDenied', self.s3_conn.put_object, Bucket=args.aws_s3_bucket, Key=bad_prefix)

            def test_get_create_file(self):
                response = self.s3_conn.put_object(Bucket=args.aws_s3_bucket, Key=self.prefix)
                self.assertTrue(self.check_response(response))

                file = self.prefix + "somerandomfile.txt"
                file_body = "foo12345\n"
                response = self.s3_conn.put_object(Bucket=args.aws_s3_bucket, Key=file, Body=file_body)
                self.assertTrue(self.check_response(response))

                response = self.s3_conn.get_object(Bucket=args.aws_s3_bucket, Key=file)
                self.assertTrue(self.check_response(response, expected_keys=('Body',)))
                self.assertEqual(response['Body'].read(), file_body)

            def test_list_files(self):
                response = self.s3_conn.list_objects(Bucket=args.aws_s3_bucket, MaxKeys=60, Prefix=self.prefix)
                self.assertTrue(self.check_response(response))

            def test_dynamo_table_create_denied(self):
                self.raisesClientError('AccessDeniedException',
                                       self.dynamodb.create_table,
                                       AttributeDefinitions=[{'AttributeName': 'Foo', 'AttributeType': 'Bar',}],
                                       TableName='foo',
                                       KeySchema=[{'AttributeName': 'Foo', 'KeyType': 'Bar'}],
                                       ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})

            def test_dynamo_table_list_denied(self):
                self.raisesClientError('AccessDeniedException', self.dynamodb.list_tables)

            def test_dynamo_write(self):
                log_table_name = TelemetryTable.full_table_name('log')
                now = str(UtcDateTime(datetime.now()).toticks())
                item = LogTable.format_item(client=self.my_id,
                                            timestamp=now,
                                            uploaded_at=now,
                                            event_type='DEBUG',
                                            message='Test Message from Telemetry.scripts.AWS.cognito.TestAccess',
                                            thread_id=12)
                response = self.dynamodb.put_item(TableName=log_table_name, Item=item)
                self.assertEqual(self.check_response(response), {}) # the reply is empty, so assertTrue won't work
                self.dynamo_created_items.append({'TableName': log_table_name,
                                                  'Item': item})

            def test_dynamo_delete_denied(self):
                log_table_name = TelemetryTable.full_table_name('log')
                now = str(UtcDateTime(datetime.now()).toticks())
                item = LogTable.format_item(client=self.my_id,
                                            timestamp=now,
                                            uploaded_at=now,
                                            event_type='DEBUG',
                                            message='Test Message from Telemetry.scripts.AWS.cognito.TestAccess',
                                            thread_id=12)
                response = self.dynamodb.put_item(TableName=log_table_name, Item=item)
                self.assertEqual(self.check_response(response), {}) # the reply is empty, so assertTrue won't work
                self.dynamo_created_items.append({'TableName': log_table_name,
                                                  'Item': item})

                self.raisesClientError('AccessDeniedException',
                                       self.dynamodb.delete_item,
                                       TableName=log_table_name,
                                       Key={'id': item['id']})

        suite = TestLoader().loadTestsFromTestCase(TestCases)
        runner = TextTestRunner(verbosity=2)
        runner.run(suite)

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
    parser.add_argument('--aws-account-id', help='AWS Account ID. A number.', default=None)
    parser.add_argument('--region', help='Set the region for the connection. Default: us-east-1', default='us-east-1', type=str)
    parser.add_argument('--config', '-c',
                       help='Configuration that contains an AWS section',
                       default=None)
    parser.add_argument('-v', '--verbose', help='Verbose output', action='store_true', default=False)
    parser.add_argument('-d', '--debug', help='Debug output', action='store_true', default=False)
    parser.add_argument('--boto-debug', help='Debug output from boto/botocore', action='store_true', default=False)
    subparser = parser.add_subparsers()

    for sub in sub_modules:
        sub.add_arguments(parser, subparser)

    logger.setLevel(logging.DEBUG)
    args = parser.parse_args()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    if args.debug:
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
    elif args.verbose:
        handler.setLevel(logging.INFO)
    else:
        handler.setLevel(logging.WARN)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if args.config:
        config_file = Config(args.config)
        AwsConfig(config_file).read(args)

    if not args.aws_access_key_id or not args.aws_secret_access_key:
        logger.error("No access-key or secret key. Need either a config or aws_access_key_id and aws_secret_access_key.")
        sys.exit(1)

    if not args.func:
        raise ValueError('no function defined for argument')

    logger.info("Using AWS Key Id %s" % args.aws_access_key_id)

    ret = args.func(args)
    if ret is None:
        sys.exit(0)

    if not ret:
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
