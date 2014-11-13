import unittest
from expression import *
import Parse


class TestSimpleExpression(unittest.TestCase):
    def check_simple(self, expr, field, selector, value):
        self.assertEqual(field, expr.field)
        self.assertEqual(selector, expr.selector)
        self.assertEqual(value, expr.value)

    def check_selector(self, sel, op, value):
        self.assertEqual(op, sel.op)
        self.assertEqual(value, sel.value)

    def test_binary_operator(self):
        expr = Expression.parse('message == Hello')
        self.check_simple(expr, 'message', Parse.query.SelectorEqual, 'Hello')

        expr = Expression.parse('message != Hello')
        self.check_simple(expr, 'message', Parse.query.SelectorNotEqual, 'Hello')

        expr = Expression.parse('message <= Hello')
        self.check_simple(expr, 'message', Parse.query.SelectorLessThanEqual, 'Hello')

        expr = Expression.parse('message >= Hello')
        self.check_simple(expr, 'message', Parse.query.SelectorGreaterThanEqual, 'Hello')

        expr = Expression.parse('message < Hello')
        self.check_simple(expr, 'message', Parse.query.SelectorLessThan, 'Hello')

        expr = Expression.parse('message > Hello')
        self.check_simple(expr, 'message', Parse.query.SelectorGreaterThan, 'Hello')

        expr = Expression.parse('message ~ Hello')
        self.check_simple(expr, 'message', Parse.query.SelectorStartsWith, 'Hello')

    def test_exists_operator(self):
        expr = Expression.parse('message ^ true')
        self.check_simple(expr, 'message', Parse.query.SelectorExists, True)

        expr = Expression.parse('message ^ True')
        self.check_simple(expr, 'message', Parse.query.SelectorExists, True)

        expr = Expression.parse('message ^ false')
        self.check_simple(expr, 'message', Parse.query.SelectorExists, False)

        expr = Expression.parse('message ^ False')
        self.check_simple(expr, 'message', Parse.query.SelectorExists, False)

        # Make sure that an invalid boolean is rejected
        self.assertRaises(ValueError, Expression.parse, 'message ^ Hello')

    def test_fields(self):
        fields = ['client', 'message', 'event_type']  # just a few common fields
        for field in fields:
            expr = Expression.parse('%s ^ true' % field)
            self.check_simple(expr, field, Parse.query.SelectorExists, True)

        # Make sure an invalid field is rejected
        self.assertRaises(ValueError, Expression.parse, 'match ^ true')

    def test_values(self):
        # Test integer
        expr = Expression.parse('ui_integer < 10')
        self.check_simple(expr, 'ui_integer', Parse.query.SelectorLessThan, 10)

        self.assertRaises(ValueError, Expression.parse, 'ui_integer < abc')

        # Test ISO-8601 UTC date time
        expr = Expression.parse('timestamp <= 2014-08-14T01:02:03.004Z')
        self.assertEqual(expr.value.datetime.year, 2014)
        self.assertEqual(expr.value.datetime.month, 8)
        self.assertEqual(expr.value.datetime.day, 14)
        self.assertEqual(expr.value.datetime.hour, 1)
        self.assertEqual(expr.value.datetime.minute, 2)
        self.assertEqual(expr.value.datetime.second, 3)
        self.assertEqual(expr.value.datetime.microsecond, 4000)

        # Test string with single and doulbe quotes
        expr = Expression.parse('message == " Hello "')
        self.check_simple(expr, 'message', Parse.query.SelectorEqual, ' Hello ')

        expr = Expression.parse("message == ' Hello, World   '")
        self.check_simple(expr, 'message', Parse.query.SelectorEqual, ' Hello, World   ')

    def test_nested_expressions(self):
        expr = Expression.parse('((client == abc123) && (event_type == INFO) && (message ~ DEV[henry]))')
        self.assertEqual('PE(CE(PE(SE(client, ==, abc123)), '
                         'PE(SE(event_type, ==, INFO)), '
                         'PE(SE(message, ~, DEV[henry]))))', str(expr))

    def test_query(self):
        query = Expression.parse('client ^ true').query()
        self.assertIn('client', query.selectors)
        self.assertEqual(1, len(query.selectors['client']))
        self.check_selector(query.selectors['client'][0], '$exists', True)

        query = Expression.parse('message == DEV').query()
        self.assertIn('message', query.selectors)
        self.assertEqual(1, len(query.selectors['message']))
        self.check_selector(query.selectors['message'][0], '$eq', 'DEV')

        query = Expression.parse('((ui_integer > 5) && (ui_integer < 10))').query()
        self.assertIn('ui_integer', query.selectors)
        self.assertEqual(2, len(query.selectors['ui_integer']))
        self.check_selector(query.selectors['ui_integer'][0], '$gt', 5)
        self.check_selector(query.selectors['ui_integer'][1], '$lt', 10)

if __name__ == '__main__':
    unittest.main()