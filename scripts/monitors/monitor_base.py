from datetime import datetime
import logging
from boto.dynamodb2.exceptions import DynamoDBError
import time
from AWS.query import Query
from AWS.connection import Connection
from misc.html_elements import *
from misc.number_formatter import pretty_number
from misc.utc_datetime import UtcDateTime


class Summary(Table):
    def __init__(self):
        Table.__init__(self)
        self.color_list.configure([None, '#faffff'])
        self.num_entries = 0

    def add_entry(self, desc, value):
        row = TableRow([TableElement(Text(desc)),
                        TableElement(Text(str(value)))])
        color = self.color_list.color()
        if color is not None:
            row.attrs['bgcolor'] = color
        self.add_row(row, advance_color=False)
        self.num_entries += 1

    def toggle_color(self):
        self.color_list.advance()


class Monitor(object):
    """
    A monitor is a module that examines telemetry events of a particular
    nature and reports information (if any). For example, an error
    monitor looks for all error logs and reports the error count, error
    rate, and a list of errors.

    This is the base class of a monitor. All derived classes must
    implement the following methods:

    1. run() - Perform queries and analyze the returned events.
    2. report() - Generate reporting information.
    3. attachment() - Optionally generate an attachment for the notification
                      email.

    The user specifies the list of monitors to run in the command line. This
    script then invokes run(), report() and attachment() for each monitor.
    """
    def __init__(self, conn, desc, prefix=None, start=None, end=None, attachment_dir=None):
        self.conn = conn
        self.desc = desc
        self.start = start
        self.end = end
        self.logger = logging.getLogger('monitor')
        self.prefix = prefix
        self.attachment_dir = attachment_dir

    def run(self):
        """
        Perform query to telemetry server. Analyze the returned events.
        """
        raise NotImplementedError()

    def report(self, summary, **kwargs):
        """
        Report the result (if any). It may display the result to stdout. A Summary
        object (which is a simple wrapper of Table) is given for this monitor to
        append to the summary section. The summary section is the first table in
        the telemetry summary email. It is a Nx2 table. A monitor can append
        as many rows as it needs. Usually, each row consists of a description
        and a value.

        A report can return a separate report that will be appended to the
        telemetry report after the summary. This must be a HTML element
        (see html_elements.py) or None (when there is no report).
        """
        raise NotImplementedError()

    def title(self):
        return self.desc[0].upper() + self.desc[1:]

    def attachment(self):
        """
        Return the file name of the attachment. If there is no attachment,
        return None. One can report the raw log from the query as attachment
        and report a summarized version in HTML table format in report().
        """
        raise NotImplementedError()

    @staticmethod
    def query_events(conn, query, count_only=False, logger=None):
        events = list()

        # Get the count first
        query.limit = None
        query.count = 1
        event_count = Query.events(query, conn)
        if count_only:
            return events, event_count

        query.limit = None
        query.count = False

        events = Query.events(query, conn)

        return events, event_count

    def query_all(self, query, count_only=False):
        return self.query_events(self.conn, query, count_only, self.logger)

    @staticmethod
    def compute_rate(count, start, end, unit):
        if start is None or end is None:
            return None
        if unit is 'sec':
            scale = 1.0
        elif unit is 'min':
            scale = 60.0
        elif unit is 'hr':
            scale = 3600.0
        else:
            raise ValueError('unit must be sec, min, or hr')
        return pretty_number(float(count) / (end - start) * scale) + ' / ' + unit

    @staticmethod
    def clone_connection(conn):
        assert isinstance(conn, Connection)
        # FIXME - Maybe AWS connection has retry built in????
        return conn.clone()

    @staticmethod
    def run_with_retries(func, desc, max_retries, exception_func=None):
        num_retries = 0
        logger = logging.getLogger('monitor')
        while num_retries <= max_retries:
            try:
                t1 = time.time()
                retval = func()
                t2 = time.time()
                logger.debug("Successful run took %s seconds", t2-t1)
                break
            except DynamoDBError, e:
                logger.error('fail to run %s (DynamoDb:%s)' % (desc, e.message))
                num_retries += 1
                if exception_func is not None:
                    exception_func()
        else:
            raise Exception('retries (%d) exhausted' % max_retries)
        return retval


def get_client_telemetry_link(prefix, client, timestamp, span=2, host="http://localhost:8000/", isT3=False):
    if isinstance(timestamp, datetime):
        timestamp = UtcDateTime(timestamp)
    if isT3:
        return '%sbugfix/%s/logs/ALL/%s/%s/%d/' % (host, prefix, client, str(timestamp), span)
    else:
        return '%sbugfix/%s/logs/%s/%s/%d/' % (host, prefix, client, str(timestamp), span)

def get_pinger_telemetry_link(prefix, timestamp, span=2, host="http://localhost:8000/"):
    if isinstance(timestamp, datetime):
        timestamp = UtcDateTime(timestamp)
    return '%spinger/%s/logs/%s/%d/' % (host, prefix, str(timestamp), span)