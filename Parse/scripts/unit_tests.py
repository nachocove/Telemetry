# This is the top-level harness for all unit test modules.
#
# To run all tests:
#
# python -m unittest -v unit_tests
#

import unittest

# Add all new unit test module here.
from html_elements_unit_tests import *
from event_formatter_unit_tests import *
from config_unit_tests import *
from number_formatter_unit_tests import *
from analytics.statistics_unit_tests import *
from Parse.unit_tests import *

if __name__ == '__main__':
    unittest.main()

