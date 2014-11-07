from query_filter import QueryFilter
from selectors import SelectorEqual, SelectorCompare, SelectorGreaterThanEqual, SelectorLessThan, SelectorBetween


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

    @staticmethod
    def optimize_range(sels):
        lo = None
        hi = None
        if len(sels) == 2:
            if isinstance(sels[0], SelectorGreaterThanEqual) and isinstance(sels[1], SelectorLessThan):
                (lo, hi) = sels
            if isinstance(sels[1], SelectorGreaterThanEqual) and isinstance(sels[0], SelectorLessThan):
                (hi, lo) = sels
        return lo, hi

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
        sels = selectors[field_name]
        (lo, hi) = TelemetryTableQuery.optimize_range(sels)
        if lo is not None and hi is not None:
            self.secondary_keys.add(field_name, SelectorBetween(lo.value, hi.value))
            return
        for sel in sels:
            TelemetryTableQuery.check_rangekey_selector(sel)
            self.secondary_keys.add(field_name, sel)

    def set_query_filter(self, query):
        self.query_filter = QueryFilter()
        fields = set(query.selectors.keys())
        for field in self.primary_keys.fields:
            if field in fields:
                fields.remove(field)
        for field in self.secondary_keys.fields:
            if field in fields:
                fields.remove(field)
        for field in fields:
            sels = query.selectors[field]
            (lo, hi) = TelemetryTableQuery.optimize_range(sels)
            if lo is not None and hi is not None:
                self.secondary_keys.add(field, SelectorBetween(lo.value, hi.value))
                continue
            for sel in sels:
                self.query_filter.add(field, sel)

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
