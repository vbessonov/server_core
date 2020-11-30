from abc import ABCMeta, abstractmethod
from enum import Enum

import six


@six.add_metaclass(ABCMeta)
class Visitor(object):
    """Interface for visitors walking through abstract syntax trees (AST)."""

    @abstractmethod
    def visit(self, node):
        """Process the specified node.

        :param node: AST node
        :type node: Node
        """
        raise NotImplementedError()


@six.add_metaclass(ABCMeta)
class Visitable(object):
    """Interface for objects walkable by AST visitors."""

    @abstractmethod
    def accept(self, visitor):
        """Accept  the specified visitor.

        :param visitor: Visitor object
        :type visitor: Visitor

        :return: Evaluated result
        :rtype: Any
        """
        raise NotImplementedError()


class Node(Visitable):
    """Base class for all AST nodes."""

    def accept(self, visitor):
        """Accept  the specified visitor.

        :param visitor: Visitor object
        :type visitor: Visitor

        :return: Evaluated result
        :rtype: Any
        """
        return visitor.visit(self)


class Identifier(Node):
    """Represents an identifier."""

    def __init__(self, value):
        """Initialize a new instance of Identifier class.

        :param value: String containing the identifier
        :type value: str
        """
        self._value = value

    @property
    def value(self):
        """Return the identifier's value.

        :return: Identifier's value
        :rtype: str
        """
        return self._value


class Number(Node):
    """Represents a number."""

    def __init__(self, value):
        """Initialize a new instance of Number class.

        :param value: Number
        :type value: Union[str, float]
        """
        self._value = value

    @property
    def value(self):
        """Return the number's value.

        :return: Number's value
        :rtype: Union[str, float]
        """
        return self._value


class Expression(Node):
    """Base class for AST nodes representing different types of expressions."""


class Operator(Enum):
    """Enumeration containing different types of available operators."""

    # Arithmetic operators
    ADDITION = "ADDITION"
    SUBTRACTION = "SUBTRACTION"
    MULTIPLICATION = "MULTIPLICATION"
    DIVISION = "DIVISION"
    EXPONENTIATION = "EXPONENTIATION"

    # Boolean operators
    INVERSION = "INVERSION"
    CONJUNCTION = "CONJUNCTION"
    DISJUNCTION = "DISJUNCTION"

    # Comparison operators
    EQUAL = "EQUAL"
    NOT_EQUAL = "NOT_EQUAL"
    GREATER = "GREATER"
    GREATER_OR_EQUAL = "GREATER_OR_EQUAL"
    LESS = "LESS"
    LESS_OR_EQUAL = "LESS_OR_EQUAL"
    IN = "IN"


class UnaryExpression(Expression):
    """Represents an unary expression."""

    def __init__(self, operator, argument):
        """Initialize a new instance of UnaryExpression class.

        :param operator: Operator
        :type operator: Operator

        :param argument: Argument
        :type argument: Node
        """
        self._operator = operator
        self._argument = argument

    @property
    def operator(self):
        """Return the expression's operator.

        :return: Expression's operator
        :rtype: Operator
        """
        return self._operator

    @property
    def argument(self):
        """Return the expression's argument.

        :return: Expression's argument
        :rtype: Node
        """
        return self._argument


class BinaryExpression(Expression):
    """Represents a binary expression."""

    def __init__(self, operator, left_argument, right_argument):
        """Initialize a new instance of BinaryExpression class.

        :param operator: Operator
        :type operator: Operator

        :param left_argument: Left argument
        :type left_argument: Node

        :param right_argument: Right argument
        :type right_argument: Node
        """
        if not isinstance(operator, Operator):
            raise ValueError(
                "Argument 'operator' must be an instance of {0} class".format(Operator)
            )

        self._operator = operator
        self._left_argument = left_argument
        self._right_argument = right_argument

    @property
    def operator(self):
        """Return the expression's operator.

        :return: Expression's operator
        :rtype: Operator
        """
        return self._operator

    @property
    def left_argument(self):
        """Return the expression's left argument.

        :return: Expression's left argument
        :rtype: Node
        """
        return self._left_argument

    @property
    def right_argument(self):
        """Return the expression's right argument.

        :return: Expression's right argument
        :rtype: Node
        """
        return self._right_argument


class UnaryArithmeticExpression(UnaryExpression):
    """Represents an unary arithmetic expression."""


class BinaryArithmeticExpression(BinaryExpression):
    """Represents a binary arithmetic expression."""


class UnaryBooleanExpression(UnaryExpression):
    """Represents an unary boolean expression."""


class BinaryBooleanExpression(BinaryExpression):
    """Represents a binary boolean expression."""


class ComparisonExpression(BinaryExpression):
    """Represents a comparison expression."""


class SliceExpression(Expression):
    """Represents a slice expression."""

    def __init__(self, array, slice_expression):
        """Initialize a new instance of SliceExpression.

        :param array: Array
        :type array: Node

        :param slice_expression: Slice expression
        :type slice_expression: Expression
        """
        self._array = array
        self._slice = slice_expression

    @property
    def array(self):
        """Return the array node.

        :return: Array node
        :rtype: Node
        """
        return self._array

    @property
    def slice(self):
        """Return the slice expression.

        :return: Slice expression
        :rtype: Expression
        """
        return self._slice


class FunctionCallExpression(Expression):
    def __init__(self, function, arguments):
        self._function = function
        self._arguments = arguments
