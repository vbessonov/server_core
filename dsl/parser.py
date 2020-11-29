import math
import operator
from abc import abstractmethod, ABCMeta

import six
from enum import Enum
from pyparsing import (
    Literal,
    CaselessKeyword,
    Forward,
    ZeroOrMore,
    Word,
    nums,
    alphas,
    alphanums,
    Suppress,
    Group,
)

from core.dsl.ast import Number, SliceExpression, Identifier, ArithmeticOperationType, BinaryArithmeticExpression, \
    ComparisonExpression, ComparisonOperationType
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
    def _parse_binary_arithmetic_expression(tokens):
        if len(tokens) >= 3:
            left_argument = tokens[0]
            operation_type = tokens[1]
            right_argument = tokens[2]
            expression = BinaryArithmeticExpression(operation_type, left_argument, right_argument)

            for tokens_chunk in chunks(tokens, 2, 3):
                operation_type = tokens_chunk[0]
                right_argument = tokens_chunk[1]

                expression = BinaryArithmeticExpression(operation_type, expression, right_argument)

            return expression


    @staticmethod
    def _parse_comparison_expression(tokens):
        if len(tokens) == 3:
            left_argument = tokens[0]
            operation_type = tokens[1]
            right_argument = tokens[2]

            return ComparisonExpression(operation_type, left_argument, right_argument)

    @staticmethod
    def _parse_slice_operation(tokens):
        array_expression = tokens[0][0]
        slice_expression = tokens[0][2]

        return SliceExpression(array_expression, slice_expression)

    @staticmethod
    def pushFirst(strg, loc, toks):
        DSLParser.exprStack.append(toks[0])

        # if toks and toks[0][0] == 'arr':
        #     return SliceExpression('arr', 0)

    @staticmethod
    def pushUMinus(strg, loc, toks):
        for t in toks:
            if t == "-":
                DSLParser.exprStack.append("unary -")
                # ~ exprStack.append( '-1' )
                # ~ exprStack.append( '*' )
            else:
                break

    point = Literal(".")
    # use CaselessKeyword for e and pi, to avoid accidentally matching
    # functions that start with 'e' or 'pi' (such as 'exp'); Keyword
    # and CaselessKeyword only match whole words
    e = CaselessKeyword("E")
    pi = CaselessKeyword("PI")
    # ~ fnumber = Combine( Word( "+-"+nums, nums ) +
    # ~ Optional( point + Optional( Word( nums ) ) ) +
    # ~ Optional( e + Word( "+-"+nums, nums ) ) )

    disjunction = Forward()
    conjunction = Forward()
    inversion = Forward()
    comparison = Forward()

    sum = Forward()
    compare_op_bitwise_or_pair = Forward()
    eq_bitwise_or = Forward()

    EQUAL = Literal("==").setParseAction(lambda _: ComparisonOperationType.EQUAL)
    NOT_EQUAL = Literal("!=").setParseAction(lambda _: ComparisonOperationType.NOT_EQUAL)
    GREATER = Literal(">").setParseAction(lambda _: ComparisonOperationType.GREATER)
    GREATER_OR_EQUAL = Literal(">=").setParseAction(lambda _: ComparisonOperationType.GREATER_OR_EQUAL)
    LESS = Literal("<").setParseAction(lambda _: ComparisonOperationType.LESS)
    LESS_OR_EQUAL = Literal("<=").setParseAction(lambda _: ComparisonOperationType.LESS_OR_EQUAL)

    eq_bitwise_or <<= EQUAL + sum
    compare_op_bitwise_or_pair = eq_bitwise_or
    comparison = (sum + ZeroOrMore(compare_op_bitwise_or_pair)).setParseAction(_parse_comparison_expression.__func__)
    inversion <<= ("not" + inversion) | comparison
    conjunction = inversion + ZeroOrMore("and" + inversion)
    disjunction = conjunction + ZeroOrMore("or" + conjunction)

    expression = disjunction

    # fnumber = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?")
    fnumber = Word(nums).setParseAction(_parse_number.__func__)
    ident = Word(alphas, alphanums + "_$.").setParseAction(_parse_identifier.__func__)

    PLUS = Literal('+').setParseAction(lambda _: ArithmeticOperationType.ADDITION)
    MINUS = Literal('-').setParseAction(lambda _: ArithmeticOperationType.SUBTRACTION)
    STAR = Literal('*').setParseAction(lambda _: ArithmeticOperationType.MULTIPLICATION)
    SLASH = Literal('/').setParseAction(lambda _: ArithmeticOperationType.DIVISION)

    LEFT_PAREN, RIGHT_PAREN = map(Suppress, "()")

    addop = PLUS | MINUS
    multop = STAR | SLASH
    expop = Literal("^")

    expr = Forward()
    slice = expression
    atom = (
        (0, None) * MINUS
        + (
                pi
                | e
                | fnumber
                | ident + LEFT_PAREN + sum + RIGHT_PAREN
                | Group(ident + Literal("[") + expression + Literal("]")).setParseAction(_parse_slice_operation.__func__)
                | ident
          ).setParseAction(
            pushFirst.__func__
        )
        | Group(LEFT_PAREN + sum + RIGHT_PAREN)
    ).setParseAction(pushUMinus.__func__)

    # by defining exponentiation as "atom [ ^ factor ]..." instead of "atom [ ^ atom ]...", we get right-to-left exponents, instead of left-to-righ
    # that is, 2^3^2 = 2^(3^2), not (2^3)^2.
    factor = Forward()
    factor << (atom + ZeroOrMore(expop + factor)).setParseAction(_parse_binary_arithmetic_expression.__func__)

    term = (factor + ZeroOrMore(multop + factor)).setParseAction(_parse_binary_arithmetic_expression.__func__)
    sum << (term + ZeroOrMore(addop + term)).setParseAction(_parse_binary_arithmetic_expression.__func__)
    bnf = expression

    epsilon = 1e-12

    opn = {
        "+": operator.add,
        "-": operator.sub,
        "*": operator.mul,
        "/": operator.truediv,
        "^": operator.pow,
    }

    fn = {
        "sin": math.sin,
        "cos": math.cos,
        "tan": math.tan,
        "exp": math.exp,
        "abs": abs,
        "trunc": lambda a: int(a),
        "round": round,
        "sgn": lambda a: (a > DSLParser.epsilon) - (a < -DSLParser.epsilon),
    }

    def evaluateStack(self, s):
        op = s.pop()
        if op == "unary -":
            return -self.evaluateStack(s)
        if op in '[]':
            slice = self.evaluateStack(s)
            return slice
        if op in "+-*/^":
            op2 = self.evaluateStack(s)
            op1 = self.evaluateStack(s)
            return DSLParser.opn[op](op1, op2)
        elif op == "==":
            op2 = self.evaluateStack(s)
            op1 = self.evaluateStack(s)
            return op1 == op2
        elif op == "PI":
            return math.pi  # 3.1415926535
        elif op == "E":
            return math.e  # 2.718281828
        elif op in DSLParser.fn:
            return DSLParser.fn[op](self.evaluateStack(s))
        elif op[0].isalpha():
            raise Exception("invalid identifier '%s'" % op)
        else:
            return float(op)

    def parse(self, string):
        self.exprStack[:] = []

        results = self.expression.parseString(string, parseAll=True)

        # return self.evaluateStack(self.exprStack[:])

        evaluator = DSLEvaluator({})
        result = evaluator.visit(results[0])

        return result
