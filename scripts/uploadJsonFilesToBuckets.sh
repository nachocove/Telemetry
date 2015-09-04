#!/bin/sh

if [ -z "$1" ] ; then
  echo "usage: uploadJsonFilesToBuckets.sh <bucket_prefix>" 
  exit 0
fi
BUCKET_PREFIX=$1
CONFIG_PREFIX=../config/db
aws s3 cp $CONFIG_PREFIX/nm_device_info_jsonpath.json s3://$BUCKET_PREFIX-t3-device-info/json/
aws s3 cp $CONFIG_PREFIX/nm_log_jsonpath.json s3://$BUCKET_PREFIX-t3-log/json/
aws s3 cp $CONFIG_PREFIX/nm_protocol_jsonpath.json s3://$BUCKET_PREFIX-t3-protocol/json/
aws s3 cp $CONFIG_PREFIX/nm_ui_jsonpath.json s3://$BUCKET_PREFIX-t3-ui/json/
aws s3 cp $CONFIG_PREFIX/nm_support_jsonpath.json s3://$BUCKET_PREFIX-t3-support/json/
aws s3 cp $CONFIG_PREFIX/nm_counter_jsonpath.json s3://$BUCKET_PREFIX-t3-counter/json/
aws s3 cp $CONFIG_PREFIX/nm_statistics2_jsonpath.json s3://$BUCKET_PREFIX-t3-statistics2/json/
aws s3 cp $CONFIG_PREFIX/nm_distribution_jsonpath.json s3://$BUCKET_PREFIX-t3-distribution/json/
aws s3 cp $CONFIG_PREFIX/nm_samples_jsonpath.json s3://$BUCKET_PREFIX-t3-samples/json/
aws s3 cp $CONFIG_PREFIX/nm_time_series_jsonpath.json s3://$BUCKET_PREFIX-t3-time-series/json/
