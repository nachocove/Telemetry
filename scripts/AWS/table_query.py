from query_filter import QueryFilter
from selectors import SelectorEqual, SelectorCompare


class TelemetryTableQuery:
    def __init__(self):
        self.for_us = False
        self.index = None
        self.secondary_keys = QueryFilter()
        self.primary_keys = QueryFilter()
        self.query_filter = QueryFilter()

    def has_index(self):
        return self.index is not None

    def has_primary_keys(self):
        return not self.primary_keys.is_empty()

    def has_secondary_keys(self):
        return not self.secondary_keys.is_empty()

    def has_keys(self):
        return self.has_primary_keys() or self.has_secondary_keys()

    def may_add_primary_hashkey(self, selectors, field_name):
        if field_name not in selectors:
            return False
        TelemetryTableQuery.check_hashkey_selector_list(selectors[field_name])
        sel = selectors[field_name][0]
        self.primary_keys.add(field_name, sel)

    def may_add_secondary_hashkey(self, selectors, field_name, index_name):
        assert isinstance(selectors, dict)
        if field_name not in selectors:
            return False
        if self.has_index():
            return False
        TelemetryTableQuery.check_hashkey_selector_list(selectors[field_name])
        sel = selectors[field_name][0]
        self.index = index_name
        self.secondary_keys.add(field_name, sel)
        return True

    def may_add_secondary_rangekey(self, selectors, field_name, index_name):
        if field_name not in selectors:
            return False
        if not self.has_index():
            return False
        if isinstance(index_name, list):
            if self.index not in index_name:
                return False
        if isinstance(index_name, str) or isinstance(index_name, unicode):
            if self.index != index_name:
                return False
        for sel in selectors[field_name]:
            TelemetryTableQuery.check_rangekey_selector(sel)
            self.secondary_keys.add(field_name, sel)

    @staticmethod
    def check_hashkey_selector_list(sels):
        if len(sels) != 1:
            raise ValueError('HashKey can only have at most one operator')
        sel = sels[0]
        if not isinstance(sel, SelectorEqual):
            raise ValueError('HashKey can only use EQ operator. (%s given)' % sel.op)

    @staticmethod
    def check_rangekey_selector(sel):
        if not issubclass(sel.__class__, SelectorCompare):
            raise ValueError('RangeKey "timestamp" can only use EQ, NE, LT, LE, GT, GE operators. (%s given)' % sel.op)
