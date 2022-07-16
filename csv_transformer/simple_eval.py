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
    def __init__(self, value: Any):
        self.value = value

    def __repr__(self):
        return "Literal({})".format(repr(self.value))

    def __eq__(self, other):
        return isinstance(other, Literal) and self.value == other.value


class Identifier:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return "Identifier({})".format(repr(self.name))

    def __eq__(self, other):
        return isinstance(other, Identifier) and self.name == other.name


class Function:
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


table_of_symbols = {
    "<": BinOp(">", -2, True, operator.lt),
    "<=": BinOp(">", -2, True, operator.le),
    "==": BinOp(">", -2, True, operator.eq),
    ">=": BinOp(">", -2, True, operator.ge),
    ">": BinOp(">", -2, True, operator.gt),
    "(": BinOp("(", -1, False, None),
    ",": BinOp(",", -1, False, None),
    ")": BinOp(")", -1, False, None),
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
    "+": BinOp("+", 0, True, operator.add),
    "-": BinOp("-", 0, True, lambda a, b: a - b),
    "/": BinOp("/", 1, True, operator.truediv),
    "*": BinOp("*", 1, True, operator.mul),
    "!": BinOp("!", 2, True, None),
    "^": BinOp("^", 2, False, operator.pow),
    ".": BinOp(".", 3, False, None),
}


# adapted from https://stackoverflow.com/a/60958017/6914441
def shunting_yard(tokens: Iterator[tokenize.TokenInfo]):
    assert next(tokens).type == ENCODING
    stack = []
    output = []

    for current_token in tokens:
        # print(current_token.string, "O", output, "S", stack)
        if current_token.type == NUMBER:
            if "." in current_token.string:
                output.append(Literal(float(current_token.string)))
            else:
                output.append(Literal(int(current_token.string)))
        elif current_token.type == STRING:
            output.append(Literal(current_token.string[1:-1]))
        elif _is_identifier(current_token):
            output.append(Identifier(current_token.string))
        elif _is_function(current_token):
            stack.append(table_of_symbols[current_token.string])
            output.append(STOP)
        elif _is_open_parenthese(current_token):
            stack.append(OPEN_PAREN)
        elif _is_close_parenthese(current_token):
            prev_op = stack.pop()
            while prev_op != OPEN_PAREN:
                if prev_op != COMMA:
                    output.append(prev_op)
                if stack:
                    prev_op = stack.pop()
                else:
                    break
            if stack and isinstance(stack[-1], Function):
                output.append(stack.pop())  # function name

        elif _is_binop(current_token):
            bin_op = table_of_symbols[current_token.string]
            if stack:
                prev_op = stack[-1]
                while prev_op != OPEN_PAREN and (
                        not (isinstance(prev_op, (Function, BinOp)))
                        or prev_op.precedence > bin_op.precedence
                        or (prev_op.precedence == bin_op.precedence
                            and bin_op.left_associative)
                ):
                    if prev_op != COMMA:
                        output.append(prev_op)
                    stack.pop()
                    if not stack:
                        break

                    prev_op = stack[-1]
            stack.append(bin_op)
        elif _is_comma(current_token):
            if stack:
                prev_op = stack[-1]
                while prev_op != OPEN_PAREN and prev_op != COMMA:
                    output.append(prev_op)
                    stack.pop()
                    if not stack:
                        break

                    prev_op = stack[-1]
            stack.append(COMMA)
        elif current_token.type == NEWLINE or current_token.type == ENDMARKER:
            pass
        else:
            raise ValueError(repr(current_token))

    output.extend(stack[::-1])
    return output


def _is_literal(current_token):
    return current_token.type == NUMBER or current_token.type == STRING


def _is_identifier(current_token):
    return (current_token.type == NAME and current_token.string
            not in table_of_symbols)


def _is_function(current_token):
    return (current_token.type == NAME and current_token.string
            in table_of_symbols)


def _is_binop(current_token):
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


def eval_expr(s: str, value_by_name: Optional[Mapping[str, Any]] = None) -> Any:
    tokens = tokenize_expr(s)
    tokens = shunting_yard(tokens)
    return evaluate(tokens, value_by_name)
