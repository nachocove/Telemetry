# Copyright 2014, NachoCove, Inc
import locale
from boto.dynamodb2.table import Table as DynamoTable
from datetime import timedelta
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

    get_metric_names={'ConsumedWriteCapacityUnits': 'Sum',
                      'ConsumedReadCapacityUnits': 'Sum',
                      'WriteThrottleEvents': 'Sum',
                      'ReadThrottleEvents': 'Sum',
                      }
    report_metric_names = ['ConsumedWriteCapacityUnits', 'ConsumedReadCapacityUnits']

    def run(self):
        self.logger.info('Querying %s...', self.desc)
        self._query()
        self._analyze()

    def per_user_cost_rounded_to_next_hour(self, cost, users, time_interval):
        assert(isinstance(time_interval, (float, int)))
        hours, seconds = divmod(int(time_interval), 3600)
        rounded_to_next_hour = hours + 1 if seconds > 0 else hours
        return (cost/users) * dynamodb_rate * rounded_to_next_hour

    def report(self, summary, **kwargs):
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

            for metric in self.report_metric_names:
                for statistic in statistics[metric]:
                    for table_index in statistics[metric][statistic]:
                        if statistics[metric][statistic][table_index]:
                            values = [float(x[statistic])/float(self.period) for x in statistics[metric][statistic][table_index]]
                            avg = numpy.average(values)
                            total_units_used[metric] += numpy.sum(values)
                            avg_element = Text(pretty_number(avg))
                            max = numpy.max(values)
                            max_element = Text(pretty_number(max))
                        else:
                            avg_element = Text('-')
                            max_element = Text('-')

                        if table_index == table_name:
                            provisioned_throughput = self.data[table_name]['info']['ProvisionedThroughput']
                            table_str = table_name
                        else:
                            table_str = "%s %s" % (table_name, table_index)
                            provisioned_throughput = None
                            for x in self.data[table_name]['info']['GlobalSecondaryIndexes']:
                                if x['IndexName'] == table_index:
                                    provisioned_throughput = x['ProvisionedThroughput']
                                    break
                        provisioned = 0.0
                        avg_throttle_element = Text('-')
                        max_throttle_element = Text('-')
                        throttle_values = None
                        if provisioned_throughput:
                            if metric == 'ConsumedWriteCapacityUnits':
                                provisioned = provisioned_throughput['WriteCapacityUnits']
                                if 'WriteThrottleEvents' in statistics and statistics['WriteThrottleEvents'][statistic][table_index]:
                                    throttle_values = [float(x[self.get_metric_names['WriteThrottleEvents']])/float(self.period) for x in statistics['WriteThrottleEvents'][statistic][table_index]]
                            elif metric == 'ConsumedReadCapacityUnits':
                                provisioned = provisioned_throughput['ReadCapacityUnits']
                                if 'ReadThrottleEvents' in statistics and statistics['ReadThrottleEvents'][statistic][table_index]:
                                    throttle_values = [float(x[self.get_metric_names['ReadThrottleEvents']])/float(self.period) for x in statistics['ReadThrottleEvents'][statistic][table_index]]
                        if throttle_values:
                            avg_throttle = numpy.average(throttle_values)
                            avg_throttle_element = Text(pretty_number(avg_throttle))
                            max_throttle = numpy.max(throttle_values)
                            max_throttle_element = Text(pretty_number(max_throttle))

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
            summary.add_entry('Consumed %s average' % k, pretty_number(total_units_used[k]))
            summary.add_entry('Per user %s average' % k, pretty_number(total_units_used[k]/user_estimate))
            summary.add_entry('Cost %s Per user' % k, self.format_cost(self.per_user_cost_rounded_to_next_hour(total_units_used[k], user_estimate, time_interval)))


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

    def _query(self):
        tables = self.conn.list_tables()[u'TableNames']
        for table_name in tables:
            if self.prefix and not table_name.startswith(self.prefix):
                continue
            if self.table and not table_name.endswith(self.table):
                continue

            table = DynamoTable(table_name=table_name, connection=self.conn)
            info = table.describe()['Table']
            statistics = {}
            self.data[table_name] = {'info': info,
                                     'statistics': {},
            }
            indexes = [x['IndexName'] for x in info['GlobalSecondaryIndexes']]
            for metric in self.get_metric_names:
                statistics[metric] = {}
                statistic = self.get_metric_names[metric]
                statistics[metric][statistic] = {}
                stats = self._get_stats(table_name,
                                        metric_name=metric,
                                        statistics=statistic)
                stats = self._get_stats(table_name,
                                        metric_name='WriteThrottled',
                                        statistics=statistic)

                statistics[metric][statistic] = {table_name: stats}
                for index in indexes:
                    stats = self._get_stats(table_name,
                                            metric_name=metric,
                                            statistics=statistic,
                                            index=index)
                    statistics[metric][statistic][index] = stats
            self.data[table_name]['statistics'] = statistics

    def _get_stats(self, table_name, metric_name, statistics, index='',
                   namespace='AWS/DynamoDB'):
        """

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
        import copy
        from datetime import datetime
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
            debug_log_args = copy.deepcopy(kwargs)
            for k in debug_log_args:
                if isinstance(debug_log_args[k], datetime):
                    debug_log_args[k] = debug_log_args[k].strftime('%Y-%m-%dT%H:%M:%S')

            dimension_str = ''
            for k in dimension:
                dimension_str += " Name=%s,Value=%s" % (k, dimension[k][0])
            debug_log_args['dimension_str'] = dimension_str

            x = self.cw_conn.get_metric_statistics(**kwargs)
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
        self.logger.debug('%s %s: %d' % (metric_name, dimension, len(stats)))
        return stats
