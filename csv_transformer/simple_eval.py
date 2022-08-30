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
import math
import operator
import random
import re
import statistics
import tokenize
from abc import ABC
from io import BytesIO
from token import ENCODING, NUMBER, STRING, NEWLINE, ENDMARKER, NAME, OP
from typing import (Any, Callable, Iterator, List, Mapping, Optional, Union,
                    Tuple)


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


class Op(ABC):
    def __init__(self, name: str, precedence: int, left_associative,
                 func: Optional[Callable]):
        self.name = name
        self.precedence = precedence
        self.left_associative = left_associative
        self.func = func


class Function(Op):
    """
    A function
    """

    def __init__(self, name: str, func: Callable):
        Op.__init__(self, name, 1, False, func)

    def __repr__(self):
        return "Function({})".format(repr(self.name))

    def __eq__(self, other):
        return isinstance(other, Function) and self.name == other.name


class BinOp(Op):
    """
    A binary operator
    """

    def __repr__(self):
        return "BinOp({})".format(repr(self.name))

    def __eq__(self, other):
        return isinstance(other, BinOp) and self.name == other.name


class PrefixUnOp(Op):
    """
    A unary operator
    """

    def __repr__(self):
        return "PrefixUnOp({})".format(repr(self.name))

    def __eq__(self, other):
        return isinstance(other, PrefixUnOp) and self.name == other.name


class InfixUnOp(Op):
    """
    A unary operator
    """

    def __repr__(self):
        return "InfixUnOp({})".format(repr(self.name))

    def __eq__(self, other):
        return isinstance(other, InfixUnOp) and self.name == other.name


OPEN_PAREN = "("
COMMA = ","
STOP = "STOP"


def tokenize_expr(s: str):
    return tokenize.tokenize(BytesIO(s.encode('utf-8')).readline)


IntoDate = Union[str, dt.date, dt.datetime]
IntoDatetime = IntoDate


def to_date(v: IntoDate) -> dt.date:
    if isinstance(v, str):
        return dt.date.fromisoformat(v)
    elif isinstance(v, dt.datetime):
        return v.date()
    elif isinstance(v, dt.date):
        return v
    else:
        raise ValueError()


def to_datetime(v: IntoDatetime) -> dt.datetime:
    if isinstance(v, str):
        return dt.datetime.fromisoformat(v)
    elif isinstance(v, dt.datetime):
        return v
    elif isinstance(v, dt.date):
        return dt.datetime(v.year, v.month, v.day)
    else:
        raise ValueError()


def to_date_or_datetime(v: IntoDatetime) -> Union[dt.date, dt.datetime]:
    ret = to_datetime(v)
    if ret.hour == 0 and ret.minute == 0 and ret.second == 0:
        return ret.date()
    else:
        return ret


# see https://en.wikipedia.org/wiki/Order_of_operations#Programming_languages
binop_by_name = {
    f.name: f for f in [
        BinOp(".", 1, False, None),
        BinOp("^", 2, False, operator.pow),
        BinOp("*", 3, True, operator.mul),
        BinOp("/", 3, True, operator.truediv),
        BinOp("%", 3, True, operator.mod),
        BinOp("+", 4, True, operator.add),
        BinOp("-", 4, True, lambda a, b: a - b),
        BinOp("<", 6, True, operator.lt),
        BinOp("<=", 6, True, operator.le),
        BinOp("==", 6, True, operator.eq),
        BinOp(">=", 6, True, operator.ge),
        BinOp(">", 6, True, operator.gt),
        BinOp("and", 11, True, operator.and_),
        BinOp("or", 12, True, operator.or_),
        BinOp("(", 15, False, None),
        BinOp(",", 15, False, None),
        BinOp(")", 15, False, None),
    ]
}

infix_unop_by_name = {}


def add_years(d: dt.date, y: int) -> dt.date:
    if isinstance(d, dt.datetime):
        return dt.datetime(d.year + y, d.month, d.day, d.hour, d.minute,
                           d.second)
    else:
        return dt.date(d.year + y, d.month, d.day)


def add_months(d: dt.date, m: int) -> dt.date:
    if isinstance(d, dt.datetime):
        return dt.datetime(d.year, d.month + m, d.day, d.hour, d.minute,
                           d.second)
    else:
        return dt.date(d.year, d.month + m, d.day)


def age(last: dt.date, first: Optional[dt.date] = None) -> Tuple[int, int, int]:
    """
    We have a first date and a second date.

    :param last:
    :param first:
    :return:
    """
    if first is None:
        first = last
        last = dt.datetime.now().date()
    years = last.year - first.year
    months = last.month - first.month
    days = last.day - first.day
    if days < 0:
        months -= 1
        if last.month == 1:
            middle = dt.date(last.year - 1, 12, first.day)
        else:
            middle = dt.date(last.year, last.month - 1, first.day)
        if isinstance(last, dt.datetime):
            days = (last.date() - middle).days
        else:
            days = (last - middle).days
    if months < 0:
        years -= 1
        months += 12
    if years < 0:
        raise ValueError()

    return years, months, days


def case(*args):
    args_count = len(args)
    assert args_count % 2 == 1
    for i in range(0, args_count - 2, 2):
        if args[i]:
            return args[i + 1]
    return args[args_count - 1]


prefix_unop_by_name = {
    f.name: f for f in [
        # https://www.postgresql.org/docs/current/functions-math.html
        Function("abs", abs),
        Function("ceil", math.ceil),
        Function("div", operator.floordiv),
        Function("exp", math.exp),
        Function("factorial", math.factorial),
        Function("floor", math.floor),
        Function("ln", math.log),
        Function("log2", math.log2),
        Function("log10", math.log10),
        Function("pi", lambda: math.pi),
        Function("round", round),
        Function("sign", lambda x: -1 if x < 0 else 1 if x > 0 else 0),
        Function("sqrt", math.sqrt),

        Function("random", random.random),
        Function("randint", random.randint),

        Function("cos", math.cos),
        Function("sin", math.sin),
        Function("tan", math.tan),
        Function("acos", math.acos),
        Function("asin", math.asin),
        Function("atan", math.atan),

        # https://www.postgresql.org/docs/current/functions-string.html
        Function("format", str.format),
        Function("len", len),
        Function("lower", str.lower),
        Function("upper", str.upper),
        Function("position", str.find),
        Function("substring", lambda s, *args: s[range(*args)]),
        Function("trim", str.strip),

        Function("max", max),
        Function("min", min),
        Function("avg", lambda *args: statistics.mean(args)),

        # https://www.postgresql.org/docs/current/functions-matching.html
        Function("re_match", re.match),
        Function("re_search", re.search),
        Function("re_first", lambda p, s: next(re.finditer(p, s), None)),

        # https://www.postgresql.org/docs/current/functions-formatting.html
        Function("int", int),
        Function("float", float),
        Function("strpdate", dt.datetime.strptime),
        Function("strfdate", dt.datetime.strftime),
        Function("str", str),
        Function("date", to_date),
        Function("datetime", to_datetime),

        # https://www.postgresql.org/docs/current/functions-datetime.html
        Function("add_years",
                 lambda d, y: add_years(to_date_or_datetime(d), y)),
        Function("add_months",
                 lambda d, y: add_months(to_date_or_datetime(d), y)),
        Function("add_days",
                 lambda d, i: to_date_or_datetime(d) + dt.timedelta(days=i)),
        Function("add_hours",
                 lambda d, i: to_datetime(d) + dt.timedelta(hours=i)),
        Function("add_minutes",
                 lambda d, i: to_datetime(d) + dt.timedelta(minutes=i)),
        Function("add_seconds",
                 lambda d, i: to_datetime(d) + dt.timedelta(seconds=i)),
        Function("age", age),
        Function("day", lambda d: to_date(d).day),
        Function("month", lambda d: to_date(d).month),
        Function("year", lambda d: to_date(d).year),
        Function("hour", lambda d: to_datetime(d).hour),
        Function("minute", lambda d: to_datetime(d).minute),
        Function("second", lambda d: to_datetime(d).second),

        # https://www.postgresql.org/docs/current/functions-conditional.html
        Function("if", lambda x, y, z: y if x else z),
        Function("case", case),

        # ops
        PrefixUnOp("-", 2, False, operator.neg),
        PrefixUnOp("!", 2, False, operator.not_),
    ]
}


# adapted from https://stackoverflow.com/a/60958017/6914441
# and https://softwareengineering.stackexchange.com/a/290975/255475
def shunting_yard(tokens: Iterator[tokenize.TokenInfo],
                  debug: bool = False) -> List[Union[Literal, Identifier,
                                                     PrefixUnOp, BinOp,
                                                     Function]]:
    return ShuntingYard(debug).process(tokens)


class ShuntingYard:
    def __init__(self, debug: bool):
        self._debug = debug
        self._operand_stack = []
        self._operator_stack = []
        self._expect_binop = False

    def process(self, tokens: Iterator[tokenize.TokenInfo]
                ) -> List[Union[Literal, Identifier, PrefixUnOp, BinOp,
                                Function]]:
        assert next(tokens).type == ENCODING

        for current_token in tokens:
            if self._debug:
                print("T: {}".format(current_token.string))

            # expect binop cases: number, string, identifier
            # or closed parenthese
            if current_token.type == NUMBER:
                if "." in current_token.string:
                    literal = Literal(float(current_token.string))
                else:
                    literal = Literal(int(current_token.string))
                self._operand_stack.append(literal)
                self._expect_binop = True
            elif current_token.type == STRING:
                self._operand_stack.append(Literal(current_token.string[1:-1]))
                self._expect_binop = True
            elif self._is_identifier(current_token):
                self._operand_stack.append(Identifier(current_token.string))
                self._expect_binop = True
            elif self._is_close_parenthese(current_token):
                self._handle_close_parenthese()
                self._expect_binop = True

            # do not expect binop
            elif self._is_function(current_token):
                self._operator_stack.append(
                    prefix_unop_by_name[current_token.string])
                self._operand_stack.append(STOP)  # put the parameters stop
                self._expect_binop = False
            elif self._is_open_parenthese(current_token):
                self._operator_stack.append(
                    OPEN_PAREN)  # place the parenthese to stop the unstacking
                self._expect_binop = False
            elif self._is_comma(current_token):
                self._handle_comma()
                self._expect_binop = False
            elif self._is_op(current_token):
                cur_op = self._get_op(current_token)
                self._handle_op(cur_op)
                self._expect_binop = False
            elif (current_token.type == NEWLINE
                  or current_token.type == ENDMARKER):
                pass
            else:
                raise ValueError(repr(current_token))
            if self._debug:
                print(">>> Opd: {} | Opr: {} | expB2: {}".format(
                    self._operand_stack, self._operator_stack,
                    self._expect_binop))

        self._operand_stack.extend(self._operator_stack[::-1])
        if self._debug:
            print("T: # | Opd: {} | Opr: [] | expB2: {}".format(
                self._operand_stack,
                self._expect_binop))
        return self._operand_stack

    @staticmethod
    def _is_close_parenthese(current_token):
        return current_token.type == OP and current_token.string == ")"

    def _handle_close_parenthese(self):
        # all operand are on the operand stack,
        # just take the operators on top of the "("
        while self._operand_stack:
            prev_op = self._operator_stack.pop()
            if prev_op == COMMA:  # ignore the commas
                continue
            elif prev_op == OPEN_PAREN:
                # this is the good "(" because
                # all intermediate "(" have been reduced
                break
            self._operand_stack.append(prev_op)
        # if it a function call, then the function has the highest precedence
        # and will be yielded next time.

    @staticmethod
    def _is_comma(current_token):
        return current_token.type == OP and current_token.string == ","

    def _handle_comma(self):
        # end of the parameters: unstack operators on top of the previous comma
        # = operator of the current parameter
        if self._operator_stack:
            prev_op = self._operator_stack[-1]
            while prev_op != OPEN_PAREN and prev_op != COMMA:
                self._operand_stack.append(prev_op)
                self._operator_stack.pop()
                if not self._operator_stack:
                    break

                prev_op = self._operator_stack[-1]
        self._operator_stack.append(COMMA)

    @staticmethod
    def _is_op(current_token):
        return current_token.type == OP and current_token.string not in (
            "(", ")", ",")

    def _get_op(self, current_token):
        if self._expect_binop:  # this is a binop or an infix unop
            try:
                cur_op = infix_unop_by_name[current_token.string]
            except KeyError:
                cur_op = binop_by_name[current_token.string]
        else:
            cur_op = prefix_unop_by_name[current_token.string]
        return cur_op

    def _handle_op(self, cur_op: Op):
        # yield every stacked operator that has a higher precedence
        if self._operator_stack:
            prev_op = self._operator_stack[-1]
            while prev_op != OPEN_PAREN:
                if (isinstance(prev_op, Op)
                        and self._has_higher_precedence(cur_op, prev_op)):
                    break
                if prev_op != COMMA:
                    self._operand_stack.append(prev_op)
                self._operator_stack.pop()
                if not self._operator_stack:
                    break

                prev_op = self._operator_stack[-1]
        self._operator_stack.append(cur_op)

    def _has_higher_precedence(self, cur_binop, prev_binop):
        return (cur_binop.precedence < prev_binop.precedence
                or (cur_binop.precedence == prev_binop.precedence
                    and not cur_binop.left_associative))

    @staticmethod
    def _is_identifier(current_token):
        return (current_token.type == NAME and current_token.string
                not in prefix_unop_by_name)

    @staticmethod
    def _is_function(current_token):
        return (current_token.type == NAME and current_token.string
                in prefix_unop_by_name)

    @staticmethod
    def _is_open_parenthese(current_token):
        return current_token.type == OP and current_token.string == "("


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
        elif isinstance(token, PrefixUnOp):
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
