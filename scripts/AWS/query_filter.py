from selectors import SelectorGreaterThanEqual, SelectorLessThan


class QueryFilter:
    def __init__(self):
        self.dict_ = dict()

    def add(self, field, sel):
        self.dict_[field + '__' + sel.op] = sel.value

    def data(self):
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