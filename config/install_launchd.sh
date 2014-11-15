#!/bin/sh

repo_path=`pushd .. > /dev/null ; pwd -P ; popd > /dev/null`
echo "Telemetry working copy at: $repo_path"
scripts_path=$repo_path/scripts
echo "Telemetry scripts at: $scripts_path"
if [ ! -d $scripts_path ]
then
  echo "ERROR: The Telemetry working copy seems to be incomplete. Cannot find $scripts_path"
  exit 1
else
  echo "$scripts_path found."
fi

source plist.sh

for j in $jobs
do
  install_plist $j
done
