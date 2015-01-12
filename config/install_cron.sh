#!/bin/sh

config_path=`dirname $0`
repo_path=`dirname $config_path`
#echo "Telemetry working copy at: $repo_path"
scripts_path=$repo_path/scripts
#echo "Telemetry scripts at: $scripts_path"
if [ ! -d $scripts_path ]
then
  echo "ERROR: The Telemetry working copy seems to be incomplete. Cannot find $scripts_path"
  exit 1
fi

echo "Copying cron definitions from $scripts_path/cron/ to /etc/cron.d/"
sudo cp $scripts_path/cron/nacho-cove /etc/cron.d/
