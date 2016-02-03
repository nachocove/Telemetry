from datetime import timedelta

from AWS.s3_telemetry import create_s3_conn

__author__ = 'azimo'
import traceback

import psycopg2
from boto.exception import S3ResponseError, EC2ResponseError, BotoServerError

from AWS.s3t3_telemetry import T3_EVENT_CLASS_FILE_PREFIXES, get_T3_date_prefixes
from misc.utc_datetime import UtcDateTime


def create_db_conn(logger, db_config):
    return psycopg2.connect(dbname=db_config['dbname'], host=db_config['host'],
                            port=db_config['port'], user=db_config['user'], password=db_config['pwd'],
                            sslmode='require')


def select(logger, cursor, sql_st):
    # need a connection with dbname=<username>_db
    try:
        # retrieving all tables in my search_path
        x = cursor.execute(sql_st)
    except Exception as err:
        logger.error(err)
    rows = cursor.fetchall()
    return rows


def delete_logs(logger, project, config, event_class, start, end, table_prefix):
    startForRS = start.datetime.strftime('%Y-%m-%d %H:%M:%S')
    endForRS = end.datetime.strftime('%Y-%m-%d %H:%M:%S')
    delete_stats = {}
    if table_prefix:
        table_prefix_for_sql = table_prefix + '_' + project + '_'
    else:
        logger.info("No table prefix specified. Not deleting from main tables...")
        return None
    try:
        logger.info("Creating connection...")
        conn = create_db_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Deleting logs from %s to %s", startForRS, endForRS)
        event_classes = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
        if not isinstance(event_classes, list):
            event_classes = [event_class]
        for event_class in event_classes:
            event_class_stats = []
            delete_stats[event_class] = event_class_stats
            table_name = "%snm_%s" % (table_prefix_for_sql, T3_EVENT_CLASS_FILE_PREFIXES[event_class])
            try:
                rows = select(logger, cursor, "select count(*) from %s" % table_name)
                for row in rows:
                    preCount = row[0]

                sql_statement = "delete from %s" % table_name
                sql_statement += " where timestamped >= '%s' and timestamped <= '%s'" % (startForRS, endForRS)

                logger.info(sql_statement)
                cursor.execute(sql_statement)
                conn.commit()
                logger.info("DELETE STATUS MESSAGE:%s", cursor.statusmessage)
                deletedCount = cursor.rowcount
                old_isolation_level = conn.isolation_level
                conn.set_isolation_level(0)
                cursor.execute("vacuum %s" % table_name)
                conn.set_isolation_level(old_isolation_level)
                logger.info("VACUUM STATUS MESSAGE:%s", cursor.statusmessage)
                conn.commit()
                rows = select(logger, cursor, "select count(*) from %s" % table_name)
                for row in rows:
                    postCount = row[0]
                event_class_stats.append({"pre_count": preCount, "post_count": postCount, "delete_count": deletedCount})
                logger.info("Deleted %d (%d->%d) rows of %s", preCount - postCount, preCount, postCount, event_class)
                conn.commit()
            except Exception as err:
                logger.error(err)
                logger.error(traceback.format_exc())
        conn.commit()
        cursor.close()
        conn.close()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())
    return delete_stats


manifest_date_lookahead = timedelta(days=3)
manifest_date_lookback = timedelta(days=3)


def get_manifest_url(logger, config, event_class, start, end):
    s3_config = config["s3_config"]
    aws_config = config["aws_config"]
    s3_conn = create_s3_conn(aws_config['aws_access_key_id'], aws_config['aws_secret_access_key'])
    bucket_name = s3_config[event_class.lower()]["t3_bucket"]
    bucket = s3_conn.get_bucket(bucket_name)
    s3_files = []
    date_prefixes = get_T3_date_prefixes(start - manifest_date_lookback, end + manifest_date_lookahead)
    for prefix in date_prefixes:
        keys = bucket.list(prefix=prefix)
        for key in keys:
            last_modified = UtcDateTime(key.last_modified)
            if start <= last_modified < end:
                s3_files.append(key.name)
    return s3_files


# upload logs
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
            event_class_name = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
            table_name = "%snm_%s" % (table_prefix_for_sql, event_class_name)
            logger.info("Uploading logs to table %s from %s to %s", table_name, start, end)
            date_prefixes = get_T3_date_prefixes(start, end)
            for date_prefix in date_prefixes:
                sql_statement = "COPY %s FROM 's3://%s/%s' \
                CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' \
                gzip maxerror 100000\
                json 's3://%s/%s'" % (table_name, s3_config[event_class_name]["t3_bucket"], date_prefix,
                                      aws_config["aws_access_key_id"], aws_config["aws_secret_access_key"],
                                      s3_config[event_class_name]["t3_bucket"],
                                      s3_config[event_class_name]["t3_jsonpath"])
                try:
                    logger.info(sql_statement)
                    cursor.execute(sql_statement)
                    logger.info("STATUS MESSAGE:%s",
                                cursor.statusmessage)  # we dont get back a row count, no errors means we are good
                    conn.commit()
                    cursor.execute("select pg_last_copy_count();")
                    rows = cursor.fetchall()
                    rowsCopied = 0
                    for row in rows:
                        rowsCopied = row[0]
                        event_class_stats.append(
                            {"date": UtcDateTime(date_prefix).datetime.strftime('%Y-%m-%d'), "count": row[0]})
                    logger.info("Copied %s rows of %s for %s", rowsCopied, event_class, date_prefix)
                except Exception as err:
                    logger.error(err)
                    logger.error(traceback.format_exc())
        conn.commit()
        cursor.close()
        conn.close()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())
    return upload_stats


# create tables
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
            event_class_name = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
            table_sql_file = config["db_sql"][event_class_name]
            with open(table_sql_file, "r") as myfile:
                table_sql = myfile.read()
            table_prefix_for_sql = "%s_%s_" % (table_prefix, project) if table_prefix else "%s_" % project
            table_sql = table_sql % table_prefix_for_sql
            logger.info(table_sql)
            cursor.execute(table_sql)
            logger.info("STATUS MESSAGE:%s", cursor.statusmessage)  # we dont get back a row count, no errors means we are good
        conn.commit()
        cursor.close()
        conn.close()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())


def list_tables(logger, config):
    logger.info("Listing tables...")
    conn = create_db_conn(logger, config["db_config"])
    conn.autocommit = False
    cursor = conn.cursor()
    sql_statement = "SELECT table_schema,table_name FROM information_schema.tables ORDER BY table_schema,table_name;"
    try:
        logger.info(sql_statement)
        tables = select(logger, cursor, sql_statement)
        conn.commit()
        cursor.close()
        conn.close()
        return tables
    except Exception as err:
        logger.error(err)
        logger.error(traceback.format_exc())
        return None

def delete_tables(logger, project, config, table_prefix):
    try:
        logger.info("Creating connection...")
        conn = create_db_conn(logger, config["db_config"])
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Deleting tables...")
        event_classes = T3_EVENT_CLASS_FILE_PREFIXES["ALL"]
        for event_class in event_classes:
            event_class_name = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
            table_prefix_for_sql = "%s_%s_nm_%s" % (table_prefix, project, event_class_name)
            table_sql = "DROP TABLE IF EXISTS %s" % table_prefix_for_sql
            try:
                logger.info(table_sql)
                cursor.execute(table_sql)
                logger.info("STATUS MESSAGE:%s", cursor.statusmessage)  # we dont get back a row count, no errors means we are good
            except Exception as err:
                logger.error(err)
                logger.error(traceback.format_exc())
        conn.commit()
        cursor.close()
        conn.close()
    except (BotoServerError, S3ResponseError, EC2ResponseError) as e:
        logger.error("Error :%s(%s):%s" % (e.error_code, e.status, e.message))
        logger.error(traceback.format_exc())
