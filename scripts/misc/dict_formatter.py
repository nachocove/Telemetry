class DictFormatter:
    def __init__(self):
        self._indent_level = 0
        self._indent = ''
        self.output = ''
        self._dict = []

    def line(self, s):
        self.output += self._indent + s + '\n'

    def field_value(self, field, value):
        self.line(field + ': ' + unicode(value))

    def field_dict(self, field):
        assert len(self._dict) > 0
        assert field in self._dict[-1]
        self.field_value(field, self._dict[-1][field])

    def increase_indent(self, level=2):
        self._indent_level += 2
        self._indent = ' ' * self._indent_level

    def decrease_indent(self, level=2):
        self._indent_level -= 2
        self._indent = ' ' * self._indent_level

    def push_dict(self, dict_):
        assert dict_ is None or isinstance(dict_, dict)
        self._dict.append(dict_)

    def pop_dict(self):
        self._dict.pop()
