from abc import ABCMeta, abstractmethod

import six
from enum import Enum


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
        """
        raise NotImplementedError()


class Node(object):
    def accept(self, visitor):
        return visitor.visit(self)


class Identifier(Node):
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value


class Number(Node):
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value


class Expression(Node):
    pass


class BinaryExpression(Expression):
    """Represents a binary expression."""

    def __init__(self, operation_type, operation_left_argument, operation_right_argument):
        """Initialize a new instance of BinaryExpression class.

        :param operation_type: Operation's type"""
        self._type = operation_type
        self._left_argument = operation_left_argument
        self._right_argument = operation_right_argument

    @property
    def type(self):
        return self._type

    @property
    def left_argument(self):
        return self._left_argument

    @property
    def right_argument(self):
        return self._right_argument


class ArithmeticOperationType(Enum):
    ADDITION = 'ADDITION'
    SUBTRACTION = 'SUBTRACTION'
    MULTIPLICATION = 'MULTIPLICATION'
    DIVISION = 'DIVISION'
    EXPONENTIATION = 'EXPONENTIATION'


class BinaryArithmeticExpression(Expression):
    def __init__(self, operation_type, operation_left_argument, operation_right_argument):
        self._type = operation_type
        self._left_argument = operation_left_argument
        self._right_argument = operation_right_argument

    @property
    def type(self):
        return self._type

    @property
    def left_argument(self):
        return self._left_argument

    @property
    def right_argument(self):
        return self._right_argument


class ComparisonOperationType(Enum):
    EQUAL = 'EQUAL'
    NOT_EQUAL = 'NOT_EQUAL'
    GREATER = 'GREATER'
    GREATER_OR_EQUAL = 'GREATER_OR_EQUAL'
    LESS = 'LESS'
    LESS_OR_EQUAL = 'LESS_OR_EQUAL'


class ComparisonExpression(Expression):
    def __init__(self, operation_type, operation_left_argument, operation_right_argument):
        self._type = operation_type
        self._left_argument = operation_left_argument
        self._right_argument = operation_right_argument

    @property
    def type(self):
        return self._type

    @property
    def left_argument(self):
        return self._left_argument

    @property
    def right_argument(self):
        return self._right_argument


class BooleanOperationType(Enum):
    INVERSION = 'INVERSION'
    CONJUNCTION = 'CONJUNCTION'
    DISJUNCTION = 'DISJUNCTION'


class BooleanExpression(object):
    def __init__(self, operation_type):
        if not isinstance(operation_type, BooleanOperationType):
            raise ValueError("Argument 'operation_type' must be an instance of {0} class".format(BooleanOperationType))

        self._type = type

    @property
    def type(self):
        return self._type


class UnaryBooleanExpression(BooleanExpression):
    def __init__(self, operation_type, operation_argument):
        super(UnaryBooleanExpression, self).__init__(operation_type)

        self._argument = operation_argument


class BinaryBooleanExpression(BooleanExpression):
    def __init__(self, operation_type, operation_left_argument, operation_right_argument):
        super(BinaryBooleanExpression, self).__init__(operation_type)

        self._left_argument = operation_left_argument
        self._right_argument = operation_right_argument


class SliceExpression(Expression):
    def __init__(self, array, slice_expression):
        self._array = array
        self._slice = slice_expression


