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
        return self.primary_keys.is_empty()

    def has_secondary_keys(self):
        return self.secondary_keys.is_empty()

    def has_keys(self):
        return self.has_primary_keys() or self.has_secondary_keys()

    def may_add_primary_hashkey(self, field, field_name, sel):
        if field != field_name:
            return
        TelemetryTableQuery.check_hashkey_selector(sel)
        self.primary_keys.add(field, sel)

    def may_add_secondary_hashkey(self, field, field_name, index_name, sel):
        if field != field_name:
            return False
        if self.has_index():
            return False
        TelemetryTableQuery.check_rangekey_selector(sel)
        self.index = index_name
        self.secondary_keys.add(field, sel)
        return True

    def may_add_secondary_rangekey(self, field, field_name, index_name, sel):
        if field != field_name:
            return False
        if self.has_index():
            if isinstance(index_name, list):
                if self.index not in index_name:
                    return False
            if isinstance(index_name, str) or isinstance(index_name, unicode):
                if self.index != index_name:
                    return False
        TelemetryTableQuery.check_rangekey_selector(sel)
        self.index = index_name
        self.secondary_keys.add(field, sel)

    @staticmethod
    def check_hashkey_selector(sel):
        if not isinstance(sel, SelectorEqual):
            raise ValueError('HashKey "client" can only use EQ operator. (%s given)' % sel.op)

    @staticmethod
    def check_rangekey_selector(sel):
        if not issubclass(sel, SelectorCompare):
            raise ValueError('RangeKey "timestamp" can only use EQ, NE, LT, LE, GT, GE operators. (%s given)' % sel.op)
