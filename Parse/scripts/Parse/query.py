import json
import urllib
from objects import Object
from utc_datetime import UtcDateTime


class Selector:
    def __init__(self, value):
        self.value = value
        self.op = None

    def data(self):
        return {self.op: self.value}


class SelectorCompare(Selector):
    def __init__(self, value):
        if isinstance(value, int) or isinstance(value, str):
            Selector.__init__(self, value)
        elif isinstance(value, UtcDateTime):
            Selector.__init__(self, {'__type': 'Date', 'iso': str(value)})
        else:
            raise TypeError('unsupported type %s' % value.__class__.__name__)


class SelectorEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = '$eq'

    def data(self):
        return self.value


class SelectorNotEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = '$ne'


class SelectorLessThan(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = '$lt'


class SelectorLessThanEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = '$lte'


class SelectorGreaterThan(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = '$gt'


class SelectorGreaterThanEqual(SelectorCompare):
    def __init__(self, value):
        SelectorCompare.__init__(self, value)
        self.op = '$gte'


class SelectorExists(Selector):
    def __init__(self, value):
        assert isinstance(value, bool)
        Selector.__init__(self, value)
        self.op = '$exists'


class Query:
    def __init__(self):
        self.selectors = dict()
        self.keys = []
        self.count = None
        self.limit = None
        self.skip = None

    def add(self, field, selector):
        assert issubclass(selector.__class__, Selector)
        if field in self.selectors:
            self.selectors[field].append(selector)
        else:
            self.selectors[field] = [selector]

    def where(self):
        data = dict()
        for field in self.selectors.keys():
            data[field] = dict()
            for sel in self.selectors[field]:
                if isinstance(sel, SelectorEqual):
                    # Equal is a special case because it doesn't have
                    # a dictionary. So, if we see this, we must replace
                    # the dictionary with a value and terminate the walk
                    data[field] = sel.data()
                else:
                    for (op, value) in sel.data().items():
                        data[field][op] = value
        return data

    @staticmethod
    def objects(cls, query, conn):
        data = dict()
        data['where'] = json.dumps(query.where())
        if len(query.keys) > 0:
            data['keys'] = ','.join(query.keys)
        if query.count is not None:
            assert isinstance(query.count, int)
            data['count'] = query.count
        if query.limit is not None:
            assert isinstance(query.limit, int)
            data['limit'] = query.limit
        if query.skip is not None:
            assert isinstance(query.skip, int)
            data['skip'] = query.skip

        result = conn.get('classes/' + cls + '?' + urllib.urlencode(data))
        obj_list = []
        count = None
        if 'results' not in result:
            return obj_list, count
        if 'count' in result:
            count = result['count']
        for data in result['results']:
            obj = Object(class_name=cls)
            obj.parse(data)
            obj_list.append(obj)
        return obj_list, count
