from tables import LogTable, WbxmlTable, CounterTable, CaptureTable, SupportTable, UiTable
from selectors import Selector, SelectorGreaterThanEqual, SelectorLessThan


class Query:
    TABLES = [
        LogTable,
        WbxmlTable,
        CounterTable,
        CaptureTable,
        SupportTable,
        UiTable
    ]

    def __init__(self):
        self.selectors = dict()
        self.limit = None
        self.table_query = dict()

    def add(self, field, selector):
        assert issubclass(selector.__class__, Selector)
        if field in self.selectors:
            self.selectors[field].append(selector)
        else:
            self.selectors[field] = [selector]

    def add_range(self, field, start, stop):
        if start is None and stop is None:
            self.add(field, SelectorGreaterThanEqual(0))
        else:
            if start is not None:
                self.add(field, SelectorGreaterThanEqual(start))
            if stop is not None:
                self.add(field, SelectorLessThan(stop))

    def has_field(self, field):
        return field in self.selectors

    def _split_query(self):
        """
        Split a query into a set of table queries.
        :return:
        """
        self.table_query = list()
        for table in Query.TABLES:
            self.table_query[table] = table.should_return(self)

    @staticmethod
    def objects(cls, query, conn):
        """
        This method performs query to multiple tables and combines events (chronologically)
        and return a single list of events
        :return:
        """
        # 'cls' is not used at all. it is only there for backward compatibility. Once
        # the transition to AWS is complete, we can get rid of it
        events = list()
        for (table, table_query) in query.table_query.items():
            if table_query.has_primary_keys():
                results = table.query_2(limit=query.limit,
                                        reverse=False,
                                        consistent=False,  # FIXME - revisit this later
                                        query_filter=table_query.query_filter.data(),
                                        *table_query.primary_key.data())
            elif table_query.has_secondary_keys():
                results = table.query_2(limit=query.limit,
                                        reverse=False,
                                        consistent=False,  # FIXME - revisit this later
                                        index=table_query.index,
                                        query_filter=table_query.query_filter.data(),
                                        *table_query.secondary_key.data())
            else:
                # No keys in any of the indexes. Fall back to scan
                results = table.scan(table_query.query_filter.data())
            events.extend(results)
        return events