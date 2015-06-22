__author__ = 'azimo'
import sys
import time
import traceback
import boto
import boto.vpc
import argparse
import json
from datetime import timedelta, datetime, date
from pprint import pprint
from boto.s3.key import Key
import boto.ec2
from boto.exception import S3ResponseError, EC2ResponseError, BotoServerError
from boto.s3.connection import S3Connection
from misc.emails import Email, EmailServer
from misc.html_elements import *
from monitors.monitor_base import Summary
from django.conf import settings
from misc.utc_datetime import UtcDateTime
import logging
import psycopg2
from django.shortcuts import render, render_to_response
from django.template import Template, Context
from django.template import loader
from django.template.loader import render_to_string
from django.core.mail import EmailMultiAlternatives
from django.utils.html import strip_tags
from AWS.s3t3_telemetry import T3_EVENT_CLASS_FILE_PREFIXES, get_T3_date_prefixes
import os
import codecs

def create_db_conn(logger, db_config):
    conn=None
    try:
        conn=psycopg2.connect(dbname=db_config['dbname'], host=db_config['host'],
                              port=db_config['port'], user=db_config['user'], password=db_config['pwd'])
    except Exception as err:
        logger.error(err)
    return conn

def delete_logs(logger, project, config, event_class, start, end, table_prefix):
    aws_config = config["aws_config"]
    s3_config = config["s3_config"]
    startForRS = start.datetime.strftime('%Y-%m-%d %H:%M:%S')
    endForRS = end.datetime.strftime('%Y-%m-%d %H:%M:%S')
    if table_prefix:
        table_prefix_for_sql = table_prefix + '_' + project + '_'
    else:
        table_prefix_for_sql = project + '_'
    try:
        logger.info("Creating connection...")
        conn = create_db_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Deleting logs...")
        sql_statement="delete from %snm_%s" \
                      "where  timestamped >= '%s' and timestamped <= '%s'"\
                      % (table_prefix_for_sql, T3_EVENT_CLASS_FILE_PREFIXES[event_class], startForRS, endForRS)
        try:
            logger.info(sql_statement)
            cursor.execute(sql_statement)
            # we dont get back a row count, no errors means we are good
            logger.info("%s successful", cursor.statusmessage)
        except Exception as err:
            logger.error(err)
        conn.commit()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())

#upload logs
def upload_logs(logger, project, config, event_class, start, end, table_prefix=None):
    aws_config = config["aws_config"]
    s3_config = config["s3_config"]
    if table_prefix:
        table_prefix_for_sql = table_prefix + '_' + project + '_'
    else:
        table_prefix_for_sql = project + '_'
    upload_stats = {}
    try:
        logger.info("Creating connection...")
        conn = create_db_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Uploading logs...")
        event_classes = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
        if not isinstance(event_classes, list):
            event_classes = [event_class]
        for event_class in event_classes:
            event_class_stats = []
            upload_stats[event_class] = event_class_stats
            event_class_name= T3_EVENT_CLASS_FILE_PREFIXES[event_class]
            date_prefixes = get_T3_date_prefixes(start, end)
            for date_prefix in date_prefixes:
                sql_statement="COPY %snm_%s FROM 's3://%s/%s' \
                CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' \
                gzip maxerror 100000\
                json 's3://%s/%s'" % (table_prefix_for_sql, event_class_name, s3_config[event_class_name]["t3_bucket"], date_prefix,
                                      aws_config["aws_access_key_id"], aws_config["aws_secret_access_key"],
                                      s3_config[event_class_name]["t3_bucket"], s3_config[event_class_name]["t3_jsonpath"])
                try:
                    logger.info(sql_statement)
                    cursor.execute(sql_statement)
                    logger.info("STATUS MESSAGE:%s", cursor.statusmessage)# we dont get back a row count, no errors means we are good
                    conn.commit()
                    cursor.execute("select pg_last_copy_count();")
                    rows = cursor.fetchall()
                    rowsCopied = 0
                    for row in rows:
                        rowsCopied = row[0]
                        event_class_stats.append({"date":UtcDateTime(date_prefix).datetime.strftime('%Y-%m-%d'), "count":row[0]})
                    logger.info("Copied %s rows of %s for %s", rowsCopied, event_class, date_prefix)
                except Exception as err:
                    logger.error(err)
                conn.commit()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())
    return upload_stats

#create tables
def create_tables(logger, project, config, event_class, table_prefix=None):
    upload_stats = {}
    try:
        logger.info("Creating connection...")
        conn = create_db_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Creating tables...")
        event_classes = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
        if not isinstance(event_classes, list):
            event_classes = [event_class]
        for event_class in event_classes:
            event_class_stats = []
            upload_stats[event_class] = event_class_stats
            event_class_name= T3_EVENT_CLASS_FILE_PREFIXES[event_class]
            table_sql_file = config["db_sql"][event_class_name]
            with open (table_sql_file, "r") as myfile:
                table_sql=myfile.read()
            if table_prefix:
                table_prefix_for_sql = table_prefix + '_' + project + '_'
                table_sql = table_sql % table_prefix_for_sql
            else:
                table_prefix_for_sql = project + '_'
                table_sql = table_sql % table_prefix_for_sql
            try:
                logger.info(table_sql)
                cursor.execute(table_sql)
                logger.info("STATUS MESSAGE:%s", cursor.statusmessage)# we dont get back a row count, no errors means we are good
                conn.commit()
            except Exception as err:
                logger.error(err)
            conn.commit()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())
