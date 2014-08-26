import json
import urllib
import re
from objects import Object
from users import User
from utc_datetime import UtcDateTime


class Selector:
    def __init__(self, value):
        self.value = value
        self.op = None

    def data(self):
        return {self.op: self.value}

    def __setattr__(self, key, value):
        if key == 'value':
            if isinstance(value, str) or isinstance(value, int):
                self.__dict__[key] = value
            elif isinstance(value, UtcDateTime):
                self.__dict__[key] = {'__type': 'Date', 'iso': str(value)}
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
        if not isinstance(value, bool):
            raise TypeError('value must be bool')
        Selector.__init__(self, value)
        self.op = '$exists'


class SelectorContain(Selector):
    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError('value must be str')
        Selector.__init__(self, re.escape(value))
        self.op = '$regex'


class SelectorStartsWith(Selector):
    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError('value must be str')
        Selector.__init__(self, '^' + re.escape(value))
        self.op = '$regex'


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

    def urlencoding_data(self):
        """
        Return a dictionary ready for URL encoding (e.g. urllib.urlencode())
        """
        data = dict()
        data['where'] = json.dumps(self.where())
        if len(self.keys) > 0:
            data['keys'] = ','.join(self.keys)
        if self.count is not None:
            assert isinstance(self.count, int)
            data['count'] = self.count
        if self.limit is not None:
            assert isinstance(self.limit, int)
            data['limit'] = self.limit
        if self.skip is not None:
            assert isinstance(self.skip, int)
            data['skip'] = self.skip
        return data

    @staticmethod
    def _objects(cls, class_name, path, query, conn):
        data = query.urlencoding_data()
        if len(data) > 0:
            result = conn.get(path + '?' + urllib.urlencode(data))
        else:
            # Empty query has no parameters and does not need ?
            result = conn.get(path)
        obj_list = []
        count = None
        if 'results' not in result:
            return obj_list, count
        if 'count' in result:
            count = result['count']
        for data in result['results']:
            if cls.__name__ == 'User':
                obj = cls()
            else:
                obj = cls(class_name=class_name)
            obj.parse(data)
            obj_list.append(obj)
        return obj_list, count

    @staticmethod
    def objects(cls, query, conn):
        return Query._objects(Object, cls, 'classes/' + cls, query, conn)

    @staticmethod
    def users(query, conn):
        return Query._objects(User, None, 'users', query, conn)