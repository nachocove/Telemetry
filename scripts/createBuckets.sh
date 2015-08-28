#!/bin/sh

if [ -z "$1" ] ; then
  echo "usage: createBuckets.sh <prefix>" 
  exit 0
fi
PREFIX=$1
aws s3 mb s3://$PREFIX-t3-device-info
aws s3 mb s3://$PREFIX-t3-log
aws s3 mb s3://$PREFIX-t3-protocol
aws s3 mb s3://$PREFIX-t3-ui
aws s3 mb s3://$PREFIX-t3-support
aws s3 mb s3://$PREFIX-t3-counter
aws s3 mb s3://$PREFIX-t3-statistics2
aws s3 mb s3://$PREFIX-t3-distribution
aws s3 mb s3://$PREFIX-t3-samples
aws s3 mb s3://$PREFIX-t3-time-series
aws s3 mb s3://$PREFIX-t3-trouble-tickets

