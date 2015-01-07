from selectors import SelectorGreaterThanEqual, SelectorLessThan


class QueryFilter:
    def __init__(self):
        self.dict_ = dict()
        self.fields = list()

    def add(self, field, sel):
        self.dict_[field + '__' + sel.op] = sel.value
        self.fields.append(field)

    def data(self):
        if len(self.dict_) == 0:
            return None
        return self.dict_

    def add_range(self, field, start, stop):
        if start is None and stop is None:
            self.add(field, SelectorGreaterThanEqual(0))
        else:
            if start is not None:
                self.add(field, SelectorGreaterThanEqual(start))
            if stop is not None:
                self.add(field, SelectorLessThan(stop))

    def is_empty(self):
        return len(self.dict_) == 0

    def __str__(self):
        return str(self.dict_)
