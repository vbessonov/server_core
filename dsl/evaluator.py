import operator

from multipledispatch import dispatch

from core.dsl.ast import (
    BinaryArithmeticExpression,
    BinaryBooleanExpression,
    ComparisonExpression,
    Identifier,
    Number,
    Operator,
    SliceExpression,
    Visitor,
)
from core.exceptions import BaseError


class DSLEvaluationError(BaseError):
    """Raised when evaluation of a DSL expression fails."""


class DSLEvaluator(Visitor):
    ARITHMETIC_OPERATORS = {
        Operator.ADDITION: operator.add,
        Operator.SUBTRACTION: operator.sub,
        Operator.MULTIPLICATION: operator.mul,
        Operator.DIVISION: operator.truediv,
        Operator.EXPONENTIATION: operator.pow,
    }

    BOOLEAN_OPERATORS = {
        Operator.CONJUNCTION: operator.and_,
        Operator.DISJUNCTION: operator.or_,
    }

    COMPARISON_OPERATORS = {
        Operator.EQUAL: operator.eq,
        Operator.NOT_EQUAL: operator.ne,
        Operator.GREATER: operator.gt,
        Operator.GREATER_OR_EQUAL: operator.ge,
        Operator.LESS: operator.lt,
        Operator.LESS_OR_EQUAL: operator.le,
        Operator.IN: lambda a, b: operator.contains(b, a),
    }

    def __init__(self, context):
        self._context = context

    @staticmethod
    def _get_attribute_value(obj, attribute):
        if isinstance(obj, dict):
            if attribute not in obj:
                raise DSLEvaluationError("Cannot find attribute '{0}' in {1}".format(attribute, obj))

            return obj[attribute]
        else:
            if not hasattr(obj, attribute):
                raise DSLEvaluationError("Cannot find attribute '{0}' in {1}".format(attribute, obj))

            return getattr(obj, attribute)

    def _evaluate_binary_expression(self, binary_expression, available_operators):
        """Evaluate the binary expression.

        :param binary_expression: Binary expression
        :type binary_expression: core.dsl.ast.BinaryExpression

        :param available_operators: Dictionary containing available operators
        :type available_operators: Dict[core.dsl.ast.Operator, operator]

        :return: Evaluation result
        :rtype: Any
        """
        left_argument = binary_expression.left_argument.accept(self)
        right_argument = binary_expression.right_argument.accept(self)

        if binary_expression.operator not in available_operators:
            raise DSLEvaluationError(
                "Wrong operator {0}".format(binary_expression.operator)
            )

        expression_operator = available_operators[binary_expression.operator]
        result = expression_operator(left_argument, right_argument)

        return result

    @dispatch(Identifier)
    def visit(self, node):
        """Process the Identifier node.

        :param node: Identifier node
        :type node: Identifier
        """
        identifiers = node.value.split(".")
        top_identifier = identifiers[0]
        value = self._get_attribute_value(self._context, top_identifier)

        for identifier in identifiers[1:]:
            value = self._get_attribute_value(value, identifier)

        return value

    @dispatch(Number)
    def visit(self, node):
        """Process the Number node.

        :param node: Number node
        :type node: Number
        """
        return float(node.value)

    @dispatch(BinaryArithmeticExpression)
    def visit(self, node):
        """Process the BinaryArithmeticExpression node.

        :param node: BinaryArithmeticExpression node
        :type node: BinaryArithmeticExpression
        """
        return self._evaluate_binary_expression(node, self.ARITHMETIC_OPERATORS)

    @dispatch(BinaryBooleanExpression)
    def visit(self, node):
        """Process the BinaryBooleanExpression node.

        :param node: BinaryBooleanExpression node
        :type node: BinaryBooleanExpression
        """
        return self._evaluate_binary_expression(node, self.BOOLEAN_OPERATORS)

    @dispatch(ComparisonExpression)
    def visit(self, node):
        """Process the ComparisonExpression node.

        :param node: ComparisonExpression node
        :type node: ComparisonExpression
        """
        return self._evaluate_binary_expression(node, self.COMPARISON_OPERATORS)

    @dispatch(SliceExpression)
    def visit(self, node):
        """Process the SliceExpression node.

        :param node: SliceExpression node
        :type node: SliceExpression
        """
        array = node.array.accept(self)
        index = int(node.slice.accept(self))

        return operator.getitem(array, index)
