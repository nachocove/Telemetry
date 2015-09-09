#!/bin/sh

if [ -z "$1" ] ; then
  echo "unset AWS_ACCESS_KEY_ID\nunset AWS_SECRET_ACCESS_KEY\nunset AWS_SESSION_TOKEN\nunset AWS_SECURITY_TOKEN\n"
  exit 0
fi

TOKEN=$1
USER=azimo
PROFILE=azim
ACCOUNT_ID=263277746520


jsonResponse=`aws --profile $PROFILE sts get-session-token --serial-number arn:aws:iam::$ACCOUNT_ID:mfa/$USER --token $TOKEN`
if [ $? != 0 ] ; then
   exit 1
fi

echo $jsonResponse | python -c 'import json,sys; data=json.load(sys.stdin); print "export AWS_ACCESS_KEY_ID=%(AccessKeyId)s\nexport AWS_SECRET_ACCESS_KEY=%(SecretAccessKey)s\nexport AWS_SESSION_TOKEN=%(SessionToken)s\nexport AWS_SECURITY_TOKEN=%(SessionToken)s\n" % data["Credentials"]'
