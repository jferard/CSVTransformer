#  CSV Transformer - A CSV Transformer library in Python
#        Copyright (C) 2022 J. Férard <https://github.com/jferard>
#
#     This file is part of CSV Transformer.
#
#     CSV Transformer is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     CSV Transformer is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <http://www.gnu.org/licenses/>.

import datetime as dt
import operator
import statistics
import tokenize
from io import BytesIO
from token import ENCODING, NUMBER, STRING, NEWLINE, ENDMARKER, NAME, OP
from typing import Any, Callable, Iterator, List, Mapping, Optional


class Literal:
    """
    A literal: any value
    """

    def __init__(self, value: Any):
        self.value = value

    def __repr__(self):
        return "Literal({})".format(repr(self.value))

    def __eq__(self, other):
        return isinstance(other, Literal) and self.value == other.value


class Identifier:
    """
    An identifier
    """

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return "Identifier({})".format(repr(self.name))

    def __eq__(self, other):
        return isinstance(other, Identifier) and self.name == other.name


class Function:
    """
    A function
    """

    def __init__(self, name: str, func: Callable):
        self.name = name
        self.func = func
        self.precedence = -1
        self.left_associative = False

    def __repr__(self):
        return "Function({})".format(repr(self.name))

    def __eq__(self, other):
        return isinstance(other, Function) and self.name == other.name


class BinOp:
    """
    A binary operator
    """

    def __init__(self, name: str, precedence: int, left_associative: bool,
                 func: Optional[Callable]):
        self.name = name
        self.precedence = precedence
        self.left_associative = left_associative
        self.func = func

    def __repr__(self):
        return "BinOp({})".format(repr(self.name))

    def __eq__(self, other):
        return isinstance(other, BinOp) and self.name == other.name


class UnOp:
    """
    A unary operator
    """

    def __init__(self, name: str, precedence: int, prefix: bool,
                 func: Optional[Callable]):
        self.name = name
        self.precedence = precedence
        self.prefix = prefix
        self.func = func

    def __repr__(self):
        return "UnOp({})".format(repr(self.name))

    def __eq__(self, other):
        return isinstance(other, UnOp) and self.name == other.name


OPEN_PAREN = "("
COMMA = ","
STOP = "STOP"


def tokenize_expr(s: str):
    return tokenize.tokenize(BytesIO(s.encode('utf-8')).readline)


def to_date(v: Any):
    if isinstance(v, str):
        return dt.date.fromisoformat(v)
    elif isinstance(v, dt.date):
        return v
    elif isinstance(v, dt.datetime):
        return v.date()
    else:
        raise ValueError()


binop_by_name = {
    "<": BinOp(">", -2, True, operator.lt),
    "<=": BinOp(">", -2, True, operator.le),
    "==": BinOp(">", -2, True, operator.eq),
    ">=": BinOp(">", -2, True, operator.ge),
    ">": BinOp(">", -2, True, operator.gt),
    "(": BinOp("(", -1, False, None),
    ",": BinOp(",", -1, False, None),
    ")": BinOp(")", -1, False, None),
    "+": BinOp("+", 0, True, operator.add),
    "-": BinOp("-", 0, True, lambda a, b: a - b),
    "/": BinOp("/", 1, True, operator.truediv),
    "*": BinOp("*", 1, True, operator.mul),
    "^": BinOp("^", 2, False, operator.pow),
    ".": BinOp(".", 3, False, None),
}

unop_by_name = {
    "round": Function("round", round),
    "min": Function("min", min),
    "str": Function("str", str),
    "format": Function("format", str.format),
    "max": Function("max", max),
    "avg": Function("avg", lambda *args: statistics.mean(args)),
    "year": Function("year", lambda d: to_date(d).year),
    "date": Function("date", lambda d: to_date(d)),
    "month": Function("month", lambda d: to_date(d).month),
    "day": Function("day", lambda d: to_date(d).day),
    "-": UnOp("-", 2, True, operator.neg),
    "!": UnOp("!", 2, True, operator.not_),
}


# adapted from https://stackoverflow.com/a/60958017/6914441
# and https://softwareengineering.stackexchange.com/a/290975/255475
def shunting_yard(tokens: Iterator[tokenize.TokenInfo], debug=False):
    assert next(tokens).type == ENCODING
    operator_stack = []
    operand_stack = []
    expect_binop = False

    for current_token in tokens:
        if debug:
            print("T: {}".format(current_token.string))
        if current_token.type == NUMBER:
            if "." in current_token.string:
                operand_stack.append(Literal(float(current_token.string)))
            else:
                operand_stack.append(Literal(int(current_token.string)))
            expect_binop = True
        elif current_token.type == STRING:
            operand_stack.append(Literal(current_token.string[1:-1]))
            expect_binop = True
        elif _is_identifier(current_token):
            operand_stack.append(Identifier(current_token.string))
            expect_binop = True
        elif _is_function(current_token):
            operator_stack.append(unop_by_name[current_token.string])
            operand_stack.append(STOP)
            expect_binop = False
        elif _is_open_parenthese(current_token):
            operator_stack.append(OPEN_PAREN)
            expect_binop = False
        elif _is_close_parenthese(current_token):
            # all operand are on the operand stack,
            # just take the operators before the "("
            while operand_stack:
                prev_op = operator_stack.pop()
                if prev_op == COMMA:  # ignore the commas
                    continue
                elif prev_op == OPEN_PAREN:
                    # this is the good "(":
                    # all intermediate "(" have been reduced
                    break
                operand_stack.append(prev_op)
            # was it a function call ?
            if operator_stack and isinstance(operator_stack[-1], Function):
                operand_stack.append(operator_stack.pop())  # function name
            expect_binop = True
        elif _is_op(current_token):
            if expect_binop:  # this is a binop
                cur_binop = binop_by_name[current_token.string]
                # yield every stacked operator that has a higher precedence
                if operator_stack:
                    prev_op = operator_stack[-1]
                    while prev_op != OPEN_PAREN:
                        if isinstance(prev_op, (Function, BinOp)):
                            if prev_op.precedence < cur_binop.precedence:
                                break
                            if (cur_binop.precedence == prev_op.precedence
                                    and not cur_binop.left_associative
                            ):
                                break
                        if prev_op != COMMA:
                            operand_stack.append(prev_op)
                        operator_stack.pop()
                        if not operator_stack:
                            break

                        prev_op = operator_stack[-1]
                operator_stack.append(cur_binop)
            else:
                cur_unop = unop_by_name[current_token.string]
                # yield every stacked operator that has a higher precedence,
                # that is almost nothing
                if operator_stack:
                    prev_op = operator_stack[-1]
                    while prev_op != OPEN_PAREN:
                        if isinstance(prev_op, (Function, UnOp, BinOp)):
                            break
                        if prev_op != COMMA:
                            operand_stack.append(prev_op)
                        operator_stack.pop()
                        if not operator_stack:
                            break

                        prev_op = operator_stack[-1]
                operator_stack.append(cur_unop)
            expect_binop = False
        elif _is_comma(current_token):
            # end of the parameters: unstack operators until the next comma
            if operator_stack:
                prev_op = operator_stack[-1]
                while prev_op != OPEN_PAREN and prev_op != COMMA:
                    operand_stack.append(prev_op)
                    operator_stack.pop()
                    if not operator_stack:
                        break

                    prev_op = operator_stack[-1]
            operator_stack.append(COMMA)
            expect_binop = False
        elif current_token.type == NEWLINE or current_token.type == ENDMARKER:
            pass
        else:
            raise ValueError(repr(current_token))
        if debug:
            print(">>> Opd: {} | Opr: {} | expB2: {}".format(
                operand_stack, operator_stack, expect_binop))

    operand_stack.extend(operator_stack[::-1])
    if debug:
        print("T: # | Opd: {} | Opr: [] | expB2: {}".format(operand_stack,
                                                            expect_binop))
    return operand_stack


def _is_literal(current_token):
    return current_token.type == NUMBER or current_token.type == STRING


def _is_identifier(current_token):
    return (current_token.type == NAME and current_token.string
            not in unop_by_name)


def _is_function(current_token):
    return (current_token.type == NAME and current_token.string
            in unop_by_name)


def _is_op(current_token):
    return current_token.type == OP and current_token.string not in (
        "(", ")", ",")


def _is_open_parenthese(current_token):
    return current_token.type == OP and current_token.string == "("


def _is_close_parenthese(current_token):
    return current_token.type == OP and current_token.string == ")"


def _is_comma(current_token):
    return current_token.type == OP and current_token.string == ","


def evaluate(tokens: List[Any],
             value_by_name: Optional[Mapping[str, Any]] = None) -> Any:
    stack = []
    for token in tokens:
        if isinstance(token, Literal):
            stack.append(token.value)
        elif isinstance(token, Identifier):
            stack.append(value_by_name[token.name])
        elif isinstance(token, BinOp):
            second = stack.pop()
            first = stack.pop()
            stack.append(token.func(first, second))
        elif isinstance(token, UnOp):
            arg = stack.pop()
            stack.append(token.func(arg))
        elif isinstance(token, Function):
            args = []
            y = stack.pop()
            while y != STOP:
                args.insert(0, y)
                y = stack.pop()
            stack.append(token.func(*args))
        elif token is STOP:
            stack.append(token)
        else:
            raise ValueError(token)
    return stack[0]


def eval_expr(s: str, value_by_name: Optional[Mapping[str, Any]] = None,
              debug=False) -> Any:
    tokens = tokenize_expr(s)
    if debug:
        tokens = list(tokens)
        print("Ts: {}".format(tokens))
        tokens = iter(tokens)
    tokens = shunting_yard(tokens, debug)
    return evaluate(tokens, value_by_name)
