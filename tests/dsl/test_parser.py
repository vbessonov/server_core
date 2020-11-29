from nose.tools import eq_
from parameterized import parameterized

from core.dsl.parser import DSLParser


class TestDSLParser(object):
    @parameterized.expand([
        ('', '9 + 3', 12),
        ('', '9 + 3 + 3', 15),
        ('', '(9 + 3)', 12),
        # ('', '9 + 3 == 12', True),
        # ('', 'arr[0] == 12', True)
    ])
    def test(self, _, expression, expected_result):
        # Arrange
        parser = DSLParser()

        # Act
        result = parser.parse(expression)

        # Assert
        eq_(expected_result, result)
