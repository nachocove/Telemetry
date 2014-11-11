from misc.utc_datetime import UtcDateTime


class Selector:
    def __init__(self, value):
        self.value = value
        self.op = None

    def __setattr__(self, key, value):
        if key == 'value':
            if isinstance(value, str) or isinstance(value, unicode) or isinstance(value, int):
                self.__dict__[key] = value
            elif isinstance(value, UtcDateTime):
                self.__dict__[key] = value.toticks()
            elif isinstance(value, list) and isinstance(self, SelectorBetween):
                if isinstance(value[0], UtcDateTime):
                    self.__dict__[key] = [x.toticks() for x in value]
                else:
                    self.__dict__[key] = value
            else:
                raise TypeError('unsupported type %s' % value.__class__.__name__)
        else:
            self.__dict__[key] = value


class SelectorCompare(Selector):
    def __init__(self, value):
        Selector.__init__(self, value)


class SelectorEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'eq'


class SelectorNotEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'ne'


class SelectorLessThan(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'lt'


class SelectorLessThanEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'lte'


class SelectorGreaterThan(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'gt'


class SelectorGreaterThanEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'gte'


class SelectorExists(Selector):
    def __init__(self, value):
        if not isinstance(value, bool):
            raise TypeError('value must be bool')
        Selector.__init__(self, value)
        if value:
            self.op = 'not_null'
        else:
            self.op = 'null'


class SelectorContains(Selector):
    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError('value must be str')
        Selector.__init__(self, value)
        self.op = 'contains'


class SelectorNotContains(Selector):
    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError('value must be str')
        Selector.__init__(self, value)
        self.op = 'not_contains'


class SelectorStartsWith(Selector):
    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError('value must be str')
        Selector.__init__(self, value)
        self.op = 'beginswith'


class SelectorBetween(SelectorCompare):
    def __init__(self, lo, hi):
        Selector.__init__(self, [lo, hi])
        self.op = 'between'