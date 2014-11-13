import re
import sys

import Parse
from events import QUERY_FIELDS


class Expression:
    """
    LP = (
    RP = )
    F = <query_field>
    OP = <operator> - ==, >=, <=, <, >, ~, !, ^
    V = <value>

    E := LP E RP
    E := E AND E
    E := F OP V
    """
    @staticmethod
    def parse(s):
        Expression.check_balance(s)
        try:
            # First, try to parse it as a parenthesized expression
            return ParenthesizedExpression(s)
        except SyntaxError:
            pass
        try:
            # Second, try to parse it as a compound expression
            return CompoundExpression(s)
        except SyntaxError:
            pass
        # Third, try to parse it as a simple expression
        return SimpleExpression(s)

    @staticmethod
    def is_bounded(s, start, end):
        return s.startswith(start) and s.endswith(end)

    @staticmethod
    def check_balance(s):
        count = 0
        for c in s:
            if c == '(':
                count += 1
            if c == ')':
                count -= 1
                if count < 0:
                    raise SyntaxError('Unbalanced expression')
        if count != 0:
            raise SyntaxError('Unbalanced expression')

    def __init__(self):
        pass

    def query(self):
        raise NotImplementedError()


class ParenthesizedExpression(Expression):
    def __init__(self, s):
        Expression.__init__(self)
        s = s.strip()
        if not self.is_bounded(s, '(', ')'):
            raise SyntaxError()
        expr = s[1:-1].strip()
        self.expression = Expression.parse(expr)

    def query(self):
        return self.expression.query()

    def __str__(self):
        return 'PE(%s)' % str(self.expression)


class SimpleExpression(Expression):
    def __init__(self, s):
        Expression.__init__(self)

        # Strip the field
        match = re.match('(?P<field>\S+)\s+(?P<remain>.+)$', s.strip())
        self.field = match.group('field')
        if self.field not in QUERY_FIELDS.keys():
            raise ValueError('Unknown field %s' % self.field)

        # Strip the operator
        operators = {'==': Parse.query.SelectorEqual,
                     '!=': Parse.query.SelectorNotEqual,
                     '<=': Parse.query.SelectorLessThanEqual,
                     '>=': Parse.query.SelectorGreaterThanEqual,
                     '<': Parse.query.SelectorLessThan,
                     '>': Parse.query.SelectorGreaterThan,
                     '~': Parse.query.SelectorStartsWith,
                     '^': Parse.query.SelectorExists,
                     }
        match = re.match('(?P<operator>\S+)\s+(?P<value>.+)$', match.group('remain'))
        self.operator = match.group('operator')
        self.selector = operators[self.operator]
        if self.operator not in operators.keys():
            raise ValueError('Unknown operator %s' % self.operator)

        # Extract the value
        value = match.group('value').strip()
        if self.is_bounded(value, '\'', '\'') or self.is_bounded(value, '"', '"'):
            value = value[1:-1]

        # Check value's type
        if self.selector == Parse.query.SelectorExists:
            # This is a special case. The only valid value is true, True, false, False
            value = value.lower()
            if value == 'true':
                value = True
            elif value == 'false':
                value = False
            else:
                raise ValueError('Exist operator (~) must have value of true or false')
        else:
            if QUERY_FIELDS[self.field] == 'integer':
                try:
                    value = int(value)
                except ValueError:
                    raise ValueError('%s is not a valid integer' % value)
            elif QUERY_FIELDS[self.field] == 'iso8601':
                try:
                    value = misc.utc_datetime.UtcDateTime(value)
                except ValueError:
                    raise ValueError('%s is not a valid ISO-8601 UTC string' % value)
        self.value = value

    def query(self):
        query_ = Parse.query.Query()
        query_.add(self.field, self.selector(self.value))
        return query_

    def __str__(self):
        return 'SE(%s, %s, %s)' % (self.field, self.operator, self.value)


class CompoundExpression(Expression):
    def __init__(self, s):
        Expression.__init__(self)
        self.expressions = list()
        expr_list = [x.strip() for x in s.split('&&')]
        if len(expr_list) == 1:
            raise SyntaxError()
        for expr in expr_list:
            self.expressions.append(Expression.parse(expr))

    def query(self):
        query_ = Parse.query.Query()
        for expr in self.expressions:
            for (field, sel_list) in expr.query().selectors.items():
                for sel in sel_list:
                    query_.add(field, sel)
        return query_

    def __str__(self):
        return 'CE(%s)' % ', '.join([str(x) for x in self.expressions])

if __name__ == '__main__':
    print Expression.parse(sys.argv[1])
    print Expression.parse(sys.argv[1]).query().where()