from nose.tools import eq_
from parameterized import parameterized

from core.dsl.parser import DSLParser


class TestDSLParser(object):
    @parameterized.expand([
        ('', '9 + 3', 12),
        ('', '9 + 3 + 3', 15),
        ('', 'foo + bar', 12, {'foo': 9, 'bar': 3}),
        ('', 'foo.bar + 3', 12, {'foo': {'bar': 9}}),
        ('', '9 == 9 and 3 == 3', True),
        ('', '9 == 3 and 3 == 3', False),
        ('', '9 == 3 or 3 == 3', True),
        ('', '(9 + 3)', 12),
        ('', '2 * (9 + 3) * 2', 48),
        ('', '2 * 3 + 2', 8),
        ('', '2 + 2 * 3', 8),
        ('', '9 + 3 == 12', True),
        ('', 'arr[0] == 12', False, {'arr': [1, 2, 3]}),
        ('', 'arr[1] == 12', True, {'arr': [1, 12, 3]})
    ])
    def test(self, _, expression, expected_result, context=None):
        # Arrange
        parser = DSLParser()

        # Act
        result = parser.parse(expression, context)

        # Assert
        eq_(expected_result, result)
