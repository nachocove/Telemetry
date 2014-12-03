# Copyright 2014, NachoCove, Inc
import copy
import locale
from boto.dynamodb2.table import Table as DynamoTable
from datetime import timedelta, datetime
from AWS.pricing import dynamodb_rate
from misc.number_formatter import pretty_number
from monitors.monitor_base import Monitor


class MonitorCost(Monitor):
    """
    A monitor class for cost analysis.
    """
    def __init__(self, cloudwatch=None, prefix=None, *args, **kwargs):
        kwargs.setdefault('desc', 'Cost breakdown')
        Monitor.__init__(self, *args, **kwargs)
        self.cw_conn = cloudwatch
        self.prefix = prefix
        self.period = 60*5  # in seconds
        self.chunk = 60*6  # in minutes
        self.data = {}
        self.table = None  # set this for debugging a single table, if needed

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        self._query()
        self._analyze()

    def per_user_cost_rounded_to_next_hour(self, cost, users, time_interval):
        assert(isinstance(time_interval, (float, int)))
        hours, seconds = divmod(int(time_interval), 3600)
        rounded_to_next_hour = hours + 1 if seconds > 0 else hours
        return (cost/users) * dynamodb_rate * rounded_to_next_hour

    report_metric_names = ['ConsumedWriteCapacityUnits', 'ConsumedReadCapacityUnits']
    def report(self, summary, **kwargs):
        """
        Messy function to create the report. Some of this should probably move to the _analyze function.

        :param summary:
        :param kwargs:
        :return:
        """
        from misc.html_elements import Table, TableHeader, Bold, TableRow, Paragraph, TableElement, Text
        import numpy
        title = self.title()
        time_interval = self.end - self.start
        paragraphs = []
        user_estimate = 10

        total_units_used = {'ConsumedWriteCapacityUnits': 0,
                            'ConsumedReadCapacityUnits': 0,
                            }
        for table_name in self.data:
            # format: statistics[metric][statistic][table/index]
            statistics = self.data[table_name]['statistics']

            table = Table()
            table.add_row(TableRow([TableHeader(Bold('Table/Index')),
                                    TableHeader(Bold('Metric')),
                                    TableHeader(Bold('Statistic')),
                                    TableHeader(Bold('Count')),
                                    TableHeader(Bold('Average (period %ds)' % self.period)),
                                    TableHeader(Bold('Max (period %ds)' % self.period)),
                                    TableHeader(Bold('Provisioned')),
                                    TableHeader(Bold('Throttled Count Avg (period %ds)' % self.period)),
                                    TableHeader(Bold('Throttled Count Max (period %ds)' % self.period)),
                                    ]))

            # Unlike in the _query, we only loop over select fields here, i.e. self.report_metric_names.
            # We fetched multiple metrics, but we report on only some (and mix in some of the rest).
            for metric in self.report_metric_names:
                for statistic in statistics[metric]:
                    for table_index in statistics[metric][statistic]:
                        if statistics[metric][statistic][table_index]:
                            # Create the array of averages, i.e. for each 'Sum' data point, divide the datapoint by the
                            # period (self.period), which gives us the average over that period.
                            values = [float(x[statistic])/float(self.period) for x in statistics[metric][statistic][table_index]]
                            # then use numpy.average() to get the average over the 5 minute (or whatever self.period is) averages.
                            avg = numpy.average(values)
                            # create the average element for the report.
                            avg_element = Text(pretty_number(avg))
                            max = numpy.max(values)
                            max_element = Text(pretty_number(max))

                            # use numpy.sum() to sum up all the the 5 minute (or whatever self.period is) averages,
                            # so we get a total for this table or index. This will be used later to calculate the
                            # per-user costs!
                            total_units_used[metric] += numpy.sum(values)

                        else:
                            # there are no values, so just create an 'empty' element for the report.
                            avg_element = Text('-')
                            max_element = Text('-')

                        # here we need to distinguish between the 'table' itself and the 'GlobalSecondaryIndexes'
                        # if we're looking at the table, then table_name == table_index.
                        if table_index == table_name:
                            # here we're dealing with the table itself, so fetch the ProvisionedThroughput for it.
                            provisioned_throughput = self.data[table_name]['info']['ProvisionedThroughput']
                            table_str = table_name
                        else:
                            # here, we have an index, so go find the ProvisionedThroughput values for that index.
                            table_str = "%s %s" % (table_name, table_index)
                            provisioned_throughput = None
                            for x in self.data[table_name]['info']['GlobalSecondaryIndexes']:
                                if x['IndexName'] == table_index:
                                    provisioned_throughput = x['ProvisionedThroughput']
                                    break

                        throttle_values = None
                        provisioned = 0.0
                        # Based on whether we're looking at a read or write metric, fetch the provisioned unit value for it.
                        if provisioned_throughput:
                            if metric == 'ConsumedWriteCapacityUnits':
                                provisioned = provisioned_throughput['WriteCapacityUnits']
                                # Mix in the WriteThrottleEvents-Sum values, if present and non-empty.
                                if 'WriteThrottleEvents' in statistics and 'Sum' in statistics['WriteThrottleEvents'] and \
                                        statistics['WriteThrottleEvents']['Sum'][table_index]:
                                    throttle_values = [float(x['Sum'])/float(self.period) for x in statistics['WriteThrottleEvents']['Sum'][table_index]]
                            elif metric == 'ConsumedReadCapacityUnits':
                                provisioned = provisioned_throughput['ReadCapacityUnits']
                                # Mix in the ReadThrottleEvents-Sum values, if present and non-empty.
                                if 'ReadThrottleEvents' in statistics and 'Sum' in statistics['ReadThrottleEvents'] and \
                                        statistics['ReadThrottleEvents']['Sum'][table_index]:
                                    throttle_values = [float(x['Sum'])/float(self.period) for x in statistics['ReadThrottleEvents']['Sum'][table_index]]

                        avg_throttle_element = Text('-')
                        max_throttle_element = Text('-')
                        if throttle_values:
                            # create the elements for the table, if we found throttle values to display. otherwise,
                            # we set a default above.
                            avg_throttle = numpy.average(throttle_values)
                            avg_throttle_element = Text(pretty_number(avg_throttle))
                            max_throttle = numpy.max(throttle_values)
                            max_throttle_element = Text(pretty_number(max_throttle))

                        # Finally, make the table row.
                        table.add_row(TableRow([TableElement(Text(table_str)),
                                                TableElement(Text(metric)),
                                                TableElement(Text(statistic)),
                                                TableElement(Text(pretty_number(len(statistics[metric][statistic][table_index]))), align='right'),
                                                TableElement(avg_element, align='right'),
                                                TableElement(max_element, align='right'),
                                                TableElement(Text(pretty_number(provisioned)), align='right'),
                                                TableElement(avg_throttle_element, align='right'),
                                                TableElement(max_throttle_element, align='right'),
                                                ]))
            paragraphs.append(Paragraph([Bold(title + " " + table_name), table]))

        summary.add_entry('Estimated number of total users', pretty_number(user_estimate))
        for k in total_units_used:
            #summary.add_entry('Consumed %s average' % k, pretty_number(total_units_used[k]))
            #summary.add_entry('Per user %s average' % k, pretty_number(total_units_used[k]/user_estimate))
            label = 'ReadUnits' if k == 'ConsumedReadCapacityUnits' else 'WriteUnits'
            summary.add_entry('Cost of consumed %s units per user' % label, self.format_cost(self.per_user_cost_rounded_to_next_hour(total_units_used[k], user_estimate, time_interval)))


        return paragraphs

    def format_cost(self, cost):
        try:
            # try setting user's locale
            locale.setlocale(locale.LC_ALL, '')
            return locale.currency(cost, grouping=True)
        except ValueError:
            # didn't work. Set USA
            locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
            return locale.currency(cost, grouping=True)

    def attachment(self):
        return None

    def _analyze(self):
        pass

    query_metric_names={'ConsumedWriteCapacityUnits': ['Sum',],
                        'ConsumedReadCapacityUnits': ['Sum',],
                        'WriteThrottleEvents': ['Sum',],
                        'ReadThrottleEvents': ['Sum',],
                        }
    def _query(self):
        """
        This will assemble a deeply nested dict of dicts of dicts:

        Top level keys are the table names. For each table, there will be a dict of 'info' (the result
        of the 'describe table' command), and 'statistics'.

        'statistics' is a dict with keys of the metrics used (i.e. 'ConsumedWriteCapacityUnits', 'ConsumedReadCapacityUnits',
        etc. see self.query_metric_names).

        Each metric has a dict for each statistic fetched (i.e. Sum, Average, Maximum, etc from AWS).

        And within each statistic, we fetch the values for the table itself, and for all the GlobalSecondaryIndexes.

        So it looks something like the following:

        self.data = { 'table1' : { 'info': {<'describe table' info>},
                                   'statistics': { 'metric1' :  {'Sum|Average|Maximum|...' : {<table/index name> : [ datapoint,.....]},
                                                                                              <table/index name> : [ datapoint,.....]},
                                                                                              ....
                                                                                             },
                                                                },
                                                                {'Sum|Average|Maximum|...' : {<table/index name> : [ datapoint,.....]},
                                                                                              <table/index name> : [ datapoint,.....]},
                                                                                              ....
                                                                                             },
                                                                }, ....
                                                   'metric2' : {'Sum|Average|Maximum|...' : {<table/index name> : [ datapoint,.....]},
                                                                                              <table/index name> : [ datapoint,.....]},
                                                                                              ....
                                                                                             },
                                                                },
                                                                {'Sum|Average|Maximum|...' : {<table/index name> : [ datapoint,.....]},
                                                                                              <table/index name> : [ datapoint,.....]},
                                                                                              ....
                                                                                             },
                                                                }, ....
                                                },
                                },
                      'table2' : { 'info': {<'describe table' info>},
                                   'statistics': { 'metric1' :  {'Sum|Average|Maximum|...' : {<table/index name> : [ datapoint,.....]},
                                                                                              <table/index name> : [ datapoint,.....]},
                                                                                              ....
                                                                                             },
                                                                },
                                                                {'Sum|Average|Maximum|...' : {<table/index name> : [ datapoint,.....]},
                                                                                              <table/index name> : [ datapoint,.....]},
                                                                                              ....
                                                                                             },
                                                                }, ....
                                                   'metric2' : {'Sum|Average|Maximum|...' : {<table/index name> : [ datapoint,.....]},
                                                                                              <table/index name> : [ datapoint,.....]},
                                                                                              ....
                                                                                             },
                                                                },
                                                                {'Sum|Average|Maximum|...' : {<table/index name> : [ datapoint,.....]},
                                                                                              <table/index name> : [ datapoint,.....]},
                                                                                              ....
                                                                                             },
                                                                }, ....
                                                },
                                },
                    ....
            }


        The loop is basically driven by the table names and the self.query_metric_names dictionary of lists.

        :return:
        """
        tables = self.conn.list_tables()[u'TableNames']
        for table_name in tables:
            # filter on prefix
            if self.prefix and not table_name.startswith(self.prefix):
                continue
            # for debugging, filter on table-name
            if self.table and not table_name.endswith(self.table):
                continue

            table = DynamoTable(table_name=table_name, connection=self.conn)
            info = table.describe()['Table']
            statistics = {}
            self.data[table_name] = {'info': info,
                                     'statistics': {},
            }

            for metric in self.query_metric_names:
                statistics[metric] = {}
                for statistic in self.query_metric_names[metric]:
                    statistics[metric][statistic] = {}

                    # Get the stats for the table.
                    stats = self._get_stats(table_name,
                                            metric_name=metric,
                                            statistics=statistic)
                    statistics[metric][statistic] = {table_name: stats}

                    # Get the stats for all the GlobalSecondaryIndexes
                    for index in [x['IndexName'] for x in info['GlobalSecondaryIndexes']]:
                        stats = self._get_stats(table_name,
                                                metric_name=metric,
                                                statistics=statistic,
                                                index=index)
                        statistics[metric][statistic][index] = stats
            self.data[table_name]['statistics'] = statistics

    def _get_stats(self, table_name, metric_name, statistics, index='',
                   namespace='AWS/DynamoDB'):
        """
        Fetch the data. AWS won't return data of more than a certain number of datapoints, so we can't fetch
        a week's worth of data, or even a day. So we break the query up into multiple queries.

        :param table_name: the table name
        :type table_name: str
        :param metric_name: the metric name
        :type metric_name: str
        :param statistics:
        :type statistics: str
        :param index:
        :type index: str
        :param namespace:
        :type namespace: str
        :return: list
        """
        DEBUG_ME = False

        dimension = {'TableName': [table_name,]}
        if index:
            dimension['GlobalSecondaryIndexName'] = [index, ]

        stats = []
        curr_time = self.start.datetime
        while curr_time < self.end.datetime:
            end_time = curr_time + timedelta(minutes=self.chunk)
            if end_time > self.end.datetime:
                end_time = self.end.datetime

            kwargs = {'period': self.period,
                      'start_time': curr_time,
                      'end_time': end_time,
                      'metric_name': metric_name,
                      'namespace': namespace,
                      'statistics': statistics,
                      'dimensions': dimension}

            x = self.cw_conn.get_metric_statistics(**kwargs)
            if DEBUG_ME:
                # create the aws cli command thay corresponds to this query
                debug_log_args = copy.deepcopy(kwargs)
                for k in debug_log_args:
                    if isinstance(debug_log_args[k], datetime):
                        debug_log_args[k] = debug_log_args[k].strftime('%Y-%m-%dT%H:%M:%S')

                dimension_str = ''
                for k in dimension:
                    dimension_str += " Name=%s,Value=%s" % (k, dimension[k][0])
                debug_log_args['dimension_str'] = dimension_str
                debug_log_args['count'] = len(x)
                self.logger.debug('AWSCLI (%(count)d): aws cloudwatch get-metric-statistics --namespace %(namespace)s '
                                  '--metric-name %(metric_name)s '
                                  '--start-time %(start_time)s '
                                  '--end-time %(end_time)s '
                                  '--period %(period)s '
                                  '--statistics %(statistics)s '
                                  '--dimensions %(dimension_str)s' % debug_log_args)


            if x:
                stats.extend(x)
            curr_time = end_time
        if DEBUG_ME:
            self.logger.debug('%s %s: %d' % (metric_name, dimension, len(stats)))
        return stats
