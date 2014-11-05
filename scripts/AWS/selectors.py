from misc.utc_datetime import UtcDateTime


class Selector:
    def __init__(self, value):
        self.value = value
        self.op = None

    def __setattr__(self, key, value):
        if key == 'value':
            if isinstance(value, str) or isinstance(value, int):
                self.__dict__[key] = value
            elif isinstance(value, UtcDateTime):
                self.__dict__[key] = value.toticks()
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
        self.op = 'EQ'


class SelectorNotEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'NE'


class SelectorLessThan(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'LT'


class SelectorLessThanEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'LE'


class SelectorGreaterThan(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'GT'


class SelectorGreaterThanEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = 'GE'


class SelectorExists(Selector):
    def __init__(self, value):
        if not isinstance(value, bool):
            raise TypeError('value must be bool')
        Selector.__init__(self, value)
        if value:
            self.op = 'NOT_NULL'
        else:
            self.op = 'NULL'


class SelectorContains(Selector):
    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError('value must be str')
        Selector.__init__(self, value)
        self.op = 'CONTAINS'


class SelectorNotContains(Selector):
    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError('value must be str')
        Selector.__init__(self, value)
        self.op = 'NOT_CONTAINS'


class SelectorStartsWith(Selector):
    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError('value must be str')
        Selector.__init__(self, '^\Q' + value + '\E')
        self.op = 'BEGINS_WITH'
