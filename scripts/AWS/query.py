from events import LogEvent, WbxmlEvent, CounterEvent, CaptureEvent, SupportEvent, UiEvent, DeviceInfoEvent
from selectors import Selector, SelectorGreaterThanEqual, SelectorLessThan, SelectorBetween
from tables import DeviceInfoTable


class Query:
    EVENT_CLASSES = [
        LogEvent,
        WbxmlEvent,
        CounterEvent,
        CaptureEvent,
        SupportEvent,
        UiEvent
    ]

    def __init__(self):
        self.selectors = dict()
        self.limit = None
        self.count = False
        self.table_query = dict()

    def add(self, field, selector):
        assert issubclass(selector.__class__, Selector)
        if field in self.selectors:
            self.selectors[field].append(selector)
        else:
            self.selectors[field] = [selector]
        self._split_query()

    def add_range(self, field, start, stop):
        if start is None and stop is None:
            self.add(field, SelectorGreaterThanEqual(0))
        elif start is not None and stop is not None:
            self.add(field, SelectorBetween(start, stop))
        else:
            if start is not None:
                self.add(field, SelectorGreaterThanEqual(start))
            if stop is not None:
                self.add(field, SelectorLessThan(stop))
        self._split_query()

    def has_field(self, field):
        return field in self.selectors

    def _split_query(self):
        """
        Split a query into a set of table queries.
        :return:
        """
        self.table_query = dict()
        for event_cls in Query.EVENT_CLASSES:
            assert event_cls.TABLE_CLASS is not None
            self.table_query[event_cls] = event_cls.TABLE_CLASS.should_handle(self)

    @staticmethod
    def _query(table, table_query, is_count, limit):
        if table_query.has_primary_keys():
            if is_count:
                results = table.query_count(limit=limit,
                                            consistent=False,  # FIXME - revisit this later
                                            query_filter=table_query.query_filter.data(),
                                            **table_query.primary_keys.data())
            else:
                results = table.query_2(limit=limit,
                                        reverse=False,
                                        consistent=False,  # FIXME - revisit this later
                                        query_filter=table_query.query_filter.data(),
                                        **table_query.primary_keys.data())
        elif table_query.has_secondary_keys():
            if is_count:
                results = table.query_count(limit=limit,
                                            consistent=False,  # FIXME - revisit this later
                                            index=table_query.index,
                                            query_filter=table_query.query_filter.data(),
                                            **table_query.secondary_keys.data())
            else:
                results = table.query_2(limit=limit,
                                        reverse=False,
                                        consistent=False,  # FIXME - revisit this later
                                        index=table_query.index,
                                        query_filter=table_query.query_filter.data(),
                                        **table_query.secondary_keys.data())
        else:
            # No keys in any of the indexes. Fall back to scan
            if is_count:
                # count += table.query_count(**table_query.query_filter.data())
                # Somehow, query_count does not like it when there is no index. Use a scan instead
                results = table.scan(**table_query.query_filter.data())
                count = 0
                for res in results:
                    count += 1
                results = count
            else:
                results = table.scan(**table_query.query_filter.data())
        return results

    @staticmethod
    def events(query, connection):
        """
        This method performs query to multiple tables and combines events (chronologically)
        and return a single list of events
        :return:
        """
        # 'cls' is not used at all. it is only there for backward compatibility. Once
        # the transition to AWS is complete, we can get rid of it
        events = list()
        count = 0
        for (event_cls, table_query) in query.table_query.items():
            if not table_query.for_us:
                continue

            table_cls = event_cls.TABLE_CLASS
            assert table_cls is not None

            table = table_cls(connection)
            table.connection.NumberRetries = 50
            if query.count:
                count += Query._query(table, table_query, is_count=True, limit=query.limit)
            else:
                results = Query._query(table, table_query, is_count=False, limit=query.limit)
                table_events = event_cls.from_db_results(table.connection, results)
                events.extend(table_events)

        # We apply the limit to each table. The combined length could exceed the query limit. If so, trim again
        if query.count:
            return max(count, query.limit)
        if len(events) > query.limit:
            events = events[:query.limit]

        # Sort the chronologically.
        return Query.sort_chronologically(events)

    @staticmethod
    def users(query, connection):
        table = DeviceInfoTable(connection)
        table_query = DeviceInfoTable.should_handle(query)
        events = Query._query(table, table_query, False, query.limit)
        if query.count:
            # Only want the # of unique client id
            clients = set()
            for event in events:
                clients.add(event['client'])
            return len(clients)
        return DeviceInfoEvent.from_db_results(connection, events)

    @staticmethod
    def sort_chronologically(events):
        return sorted(events, key=lambda x: x['timestamp'])
