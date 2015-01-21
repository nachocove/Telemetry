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
email_config=$1

if [ ! -d $scripts_path ]
then
  echo "ERROR: The Telemetry working copy seems to be incomplete. Cannot find $scripts_path"
  exit 1
fi

if [ -z "$TMPDIR" ] ; then
    TMPDIR=/tmp
fi
temp=$TMPDIR/$$.$RANDOM
cronfile=/etc/cron.d/nacho-cove
echo "Copying cron definitions from $scripts_path/cron/ to /etc/cron.d/"
m4 -DCONFIG_DIR=$config_path -DEMAIL_CFG=$email_config -DSCRIPTS_DIR=$scripts_path $scripts_path/cron/nacho-cove.template > $temp || rm -f $temp
sudo chown root $temp
sudo chgrp root $temp
sudo chmod 644 $temp
sudo mv $temp $cronfile

