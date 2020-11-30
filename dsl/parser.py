from pyparsing import (
    Forward,
    Group,
    Literal,
    Suppress,
    Word,
    ZeroOrMore,
    alphanums,
    alphas,
    nums,
)

from core.dsl.ast import (
    BinaryArithmeticExpression,
    BinaryBooleanExpression,
    ComparisonExpression,
    Identifier,
    Number,
    Operator,
    SliceExpression,
)
from core.dsl.evaluator import DSLEvaluator
from core.util import chunks


class DSLParser(object):
    exprStack = []

    @staticmethod
    def _parse_identifier(tokens):
        return Identifier(tokens[0])

    @staticmethod
    def _parse_number(tokens):
        return Number(tokens[0])

    @staticmethod
    def _parse_binary_expression(expression_type, tokens):
        if len(tokens) >= 3:
            left_argument = tokens[0]
            operation_type = tokens[1]
            right_argument = tokens[2]
            expression = expression_type(operation_type, left_argument, right_argument)

            for tokens_chunk in chunks(tokens, 2, 3):
                operation_type = tokens_chunk[0]
                right_argument = tokens_chunk[1]

                expression = expression_type(operation_type, expression, right_argument)

            return expression

    @staticmethod
    def _parse_binary_arithmetic_expression(tokens):
        return DSLParser._parse_binary_expression(BinaryArithmeticExpression, tokens)

    @staticmethod
    def _parse_binary_boolean_expression(tokens):
        return DSLParser._parse_binary_expression(BinaryBooleanExpression, tokens)

    @staticmethod
    def _parse_comparison_expression(tokens):
        return DSLParser._parse_binary_expression(ComparisonExpression, tokens)

    @staticmethod
    def _parse_parenthesized_expression(tokens):
        return tokens[0]

    @staticmethod
    def _parse_slice_operation(tokens):
        array_expression = tokens[0][0]
        slice_expression = tokens[0][1]

        return SliceExpression(array_expression, slice_expression)

    @staticmethod
    def pushUMinus(strg, loc, toks):
        for t in toks:
            if t == "-":
                DSLParser.exprStack.append("unary -")
                # ~ exprStack.append( '-1' )
                # ~ exprStack.append( '*' )
            else:
                break

    LEFT_PAREN, RIGHT_PAREN = map(Suppress, "()")
    LEFT_BRACKET, RIGHT_BRACKET = map(Suppress, "[]")

    ADDITION_OPERATOR = Literal("+").setParseAction(lambda _: Operator.ADDITION)
    SUBTRACTION_OPERATOR = Literal("-").setParseAction(lambda _: Operator.SUBTRACTION)
    ADDITIVE_OPERATOR = ADDITION_OPERATOR | SUBTRACTION_OPERATOR

    MULTIPLICATION_OPERATOR = Literal("*").setParseAction(
        lambda _: Operator.MULTIPLICATION
    )
    DIVISION_OPERATOR = Literal("/").setParseAction(lambda _: Operator.DIVISION)
    MULTIPLICATIVE_OPERATOR = MULTIPLICATION_OPERATOR | DIVISION_OPERATOR

    EQUAL_OPERATOR = Literal("==").setParseAction(lambda _: Operator.EQUAL)
    NOT_EQUAL_OPERATOR = Literal("!=").setParseAction(lambda _: Operator.NOT_EQUAL)
    GREATER_OPERATOR = Literal(">").setParseAction(lambda _: Operator.GREATER)
    GREATER_OR_EQUAL_OPERATOR = Literal(">=").setParseAction(
        lambda _: Operator.GREATER_OR_EQUAL
    )
    LESS_OPERATOR = Literal("<").setParseAction(lambda _: Operator.LESS)
    LESS_OR_EQUAL_OPERATOR = Literal("<=").setParseAction(
        lambda _: Operator.LESS_OR_EQUAL
    )
    IN_OPERATOR = Literal("in").setParseAction(lambda _: Operator.IN)
    COMPARISON_OPERATOR = (
        EQUAL_OPERATOR
        | NOT_EQUAL_OPERATOR
        | GREATER_OPERATOR
        | GREATER_OR_EQUAL_OPERATOR
        | LESS_OPERATOR
        | LESS_OR_EQUAL_OPERATOR
        | IN_OPERATOR
    )

    INVERSION_OPERATOR = Literal("not").setParseAction(lambda _: Operator.INVERSION)

    CONJUNCTION_OPERATOR = Literal("and").setParseAction(lambda _: Operator.CONJUNCTION)
    DISJUNCTION_OPERATOR = Literal("or").setParseAction(lambda _: Operator.DISJUNCTION)

    arithmetic_expression = Forward()
    comparison_expression = (
        arithmetic_expression + ZeroOrMore(COMPARISON_OPERATOR + arithmetic_expression)
    ).setParseAction(_parse_comparison_expression.__func__)

    inversion_expression = Forward()
    inversion_expression <<= (
        INVERSION_OPERATOR + inversion_expression
    ) | comparison_expression
    conjunction_expression = (
        inversion_expression + ZeroOrMore(CONJUNCTION_OPERATOR + inversion_expression)
    ).setParseAction(_parse_binary_boolean_expression.__func__)
    disjunction_expression = (
        conjunction_expression + ZeroOrMore(DISJUNCTION_OPERATOR + conjunction_expression)
    ).setParseAction(_parse_binary_boolean_expression.__func__)

    expression = disjunction_expression

    # NUMBER = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?")
    NUMBER = Word(nums).setParseAction(_parse_number.__func__)
    IDENTIFIER = Word(alphas, alphanums + "_$.").setParseAction(
        _parse_identifier.__func__
    )

    POWER_OPERATOR = Literal("**")

    function_call_expression = (
        IDENTIFIER + LEFT_PAREN + expression + RIGHT_PAREN
    )

    parenthesized_expression = Group(
        LEFT_PAREN + expression + RIGHT_PAREN
    ).setParseAction(_parse_parenthesized_expression.__func__)

    slice = expression
    slice_expression = Group(
        IDENTIFIER + LEFT_BRACKET + slice + RIGHT_BRACKET
    ).setParseAction(_parse_slice_operation.__func__)

    atom = (
        (0, None) * SUBTRACTION_OPERATOR
        + (
                NUMBER
                | function_call_expression
                | slice_expression
                | IDENTIFIER
          )
        | parenthesized_expression
    ).setParseAction(pushUMinus.__func__)

    factor = Forward()
    factor << (
            atom + ZeroOrMore(POWER_OPERATOR + factor)
    ).setParseAction(_parse_binary_arithmetic_expression.__func__)
    term = (factor + ZeroOrMore(MULTIPLICATIVE_OPERATOR + factor)).setParseAction(
        _parse_binary_arithmetic_expression.__func__
    )
    arithmetic_expression << (
        term + ZeroOrMore(ADDITIVE_OPERATOR + term)
    ).setParseAction(_parse_binary_arithmetic_expression.__func__)

    def parse(self, string, context=None):
        self.exprStack[:] = []

        results = self.expression.parseString(string, parseAll=True)
        evaluator = DSLEvaluator(context)
        result = evaluator.visit(results[0])

        return result
