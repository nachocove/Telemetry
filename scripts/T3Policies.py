__author__ = 'azimo'

import boto
import boto.vpc
import argparse
import json
import boto.ec2
import boto.ec2.elb
import boto.ec2.autoscale
from boto.exception import BotoServerError
import boto.iam
from boto.ec2.elb.attributes import AccessLogAttribute

# get region from region_name
def get_region(region_name):
    for region in boto.ec2.regions():
        if region_name == region.name:
            return region

# create iam policies
def create_t3_iam_policies(region_name, policy_arn_prefix, iam_config):
    print "Creating IAM T3 policies..."
    conn=boto.iam.connect_to_region(region_name)
    for policy_config in iam_config:
        policy_name = policy_config["name"]
        policy_arn = policy_arn_prefix + policy_name
        policy_json = json.dumps(policy_config["policy"], indent=4)
        try:
            policy = conn.get_policy(policy_arn)
        except BotoServerError:
            policy = None
        if not policy:
            print "Creating policy (%s)" % policy_name
            conn.create_policy(policy_name, policy_json)
        else:
            policy_arn = policy["get_policy_response"]["get_policy_result"]["policy"]["arn"]
            if not policy_arn:
                print "Error extracting policy arn"
            else:
                print "Updating policy (%s) for arn (%s)" % (policy_name, policy_arn)
                conn.create_policy_version(policy_arn, policy_json, set_as_default=True)


# cleanup
def cleanup(config):
    print "Cleaning up..."

# process config
def process_config(config):
    config["aws_config"]["region"] = get_region(config["aws_config"]["region_name"])

# load json config
def json_config(file_name):
    with open(file_name) as data_file:
        json_data = json.load(data_file)
    return json_data

# main
def main():
    parser = argparse.ArgumentParser(description='Create T3 related policies')
    parser.add_argument('--config', required=True, type=json_config, metavar = "config_file",
                   help='the config(json) file for the policies', )
    args = parser.parse_args()
    config = args.config
    process_config(config)
    create_t3_iam_policies(config["aws_config"]["region_name"], config["aws_config"]["policy_arn_prefix"], config["iam_config"])
if __name__ == '__main__':
    main()
