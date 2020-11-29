from multipledispatch import dispatch

from core.dsl.ast import Visitor, Identifier, Number, SliceExpression, BinaryArithmeticExpression, \
    ArithmeticOperationType, ComparisonExpression, ComparisonOperationType


class DSLEvaluator(Visitor):
    def __init__(self, context):
        self._context = context

    @dispatch(Identifier)
    def visit(self, node):
        """Process the Identifier node.

        :param node: Identifier node
        :type node: Identifier
        """
        identifiers = node.value.split('.')
        top_identifier = identifiers[0]
        value = self._context[top_identifier]

        for identifier in identifiers[1:]:
            value = getattr(value, identifier)

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
        left_argument = node.left_argument.accept(self)
        right_argument = node.right_argument.accept(self)

        if node.type == ArithmeticOperationType.ADDITION:
            return left_argument + right_argument

    @dispatch(ComparisonExpression)
    def visit(self, node):
        """Process the ComparisonExpression node.

        :param node: ComparisonExpression node
        :type node: ComparisonExpression
        """
        left_argument = node.left_argument.accept(self)
        right_argument = node.right_argument.accept(self)

        if node.type == ComparisonOperationType.EQUAL:
            return left_argument == right_argument

    @dispatch(SliceExpression)
    def visit(self, node):
        """Process the SliceExpression node.

        :param node: SliceExpression node
        :type node: SliceExpression
        """
        pass
