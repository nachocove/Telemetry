#!/bin/sh

repo_path=`pushd ../.. > /dev/null ; pwd -P ; popd > /dev/null`
echo "Telemetry working copy at: $repo_path"
if [ ! -d $repo_path/Parse/scripts ]
then
  echo "ERROR: The Telemetry working copy seems to be incomplete. Cannot find $repo_path/Parse/scripts"
  exit 1
else
  echo "$repo_path/Parse/scripts found."
fi

source plist.sh

for j in $jobs
do
  install_plist $j
done
