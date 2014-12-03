# Copyright 2014, NachoCove, Inc
from boto.dynamodb2.table import Table as DynamoTable
from datetime import timedelta
from misc.number_formatter import pretty_number
from monitors.monitor_base import Monitor


class MonitorCost(Monitor):
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

    def report(self, summary, **kwargs):
        from misc.html_elements import Table, TableHeader, Bold, TableRow, Paragraph, TableElement, Text
        import numpy
        title = self.title()
        paragraphs = []
        for table_name in self.data:
            # format: statistics[metric][statistic][table/index]
            statistics = self.data[table_name]['statistics']

            table = Table()
            table.add_row(TableRow([TableHeader(Bold('Table/Index')),
                                    TableHeader(Bold('Metric')),
                                    TableHeader(Bold('Statistic')),
                                    TableHeader(Bold('Count')),
                                    TableHeader(Bold('Average')),
                                    TableHeader(Bold('Max')),
                                    TableHeader(Bold('Provisioned')),
                                    ]))

            for metric in statistics:
                for statistic in statistics[metric]:
                    for table_index in statistics[metric][statistic]:
                        if statistics[metric][statistic][table_index]:
                            avg = numpy.average([x[statistic] for x in statistics[metric][statistic][table_index]])
                            avg_element = TableElement(Text(pretty_number(avg)), align='right')
                            max = numpy.max([x[statistic] for x in statistics[metric][statistic][table_index]])
                            max_element = TableElement(Text(pretty_number(max)), align='right')
                        else:
                            avg_element = TableElement(Text('None'))
                            max_element = TableElement(Text('None'))
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
                        if provisioned_throughput:
                            if metric == 'ConsumedWriteCapacityUnits':
                                provisioned = provisioned_throughput['WriteCapacityUnits']
                            elif metric == 'ConsumedReadCapacityUnits':
                                provisioned = provisioned_throughput['ReadCapacityUnits']
                        table.add_row(TableRow([TableElement(Text(table_str)),
                                                TableElement(Text(metric)),
                                                TableElement(Text(statistic)),
                                                TableElement(Text(pretty_number(len(statistics[metric][statistic][table_index])))),
                                                avg_element,
                                                max_element,
                                                TableElement(Text(pretty_number(provisioned)))
                                                ]))
            paragraphs.append(Paragraph([Bold(title + " " + table_name), table]))
        return paragraphs

    def attachment(self):
        return None

    def _analyze(self):
        pass

    def _query(self):
        metric_names={'ConsumedWriteCapacityUnits': 'Average',
                      'ConsumedReadCapacityUnits': 'Average',
                      }
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
            for metric in metric_names:
                statistics[metric] = {}
                statistic = metric_names[metric]
                statistics[metric][statistic] = {}
                stats = self._get_stats(table_name,
                                        metric_name=metric,
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
