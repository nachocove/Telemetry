#!/bin/sh

script_name=`basename $0`
pushd `dirname $0` > /dev/null;config_path=`pwd`;popd > /dev/null
repo_path=`dirname $config_path`
scripts_path=$repo_path/scripts

if [ -z "$1" ] ; then
  echo "ERROR: $script_name <email config>"
  exit 1
fi

if [ ! -f $config_path/$1 ] ; then
   echo "ERROR: Email config file $config_path/$1 does not exist."
   exit 1
fi

if [ ! -d $scripts_path ]
then
  echo "ERROR: The Telemetry working copy seems to be incomplete. Cannot find $scripts_path"
  exit 1
fi

if [ -z "$TMPDIR" ] ; then
    TMPDIR=/tmp
fi
temp=$TMPDIR/$$.$RANDOM

echo "Copying cron definitions from $scripts_path/cron/ to /etc/cron.d/"
m4 -DCONFIG_DIR=$config_path -DEMAIL_CFG=$1 $scripts_path/cron/nacho-cove.template > $temp || rm -f $temp
sudo mv $temp /etc/cron.d/nacho-cove

