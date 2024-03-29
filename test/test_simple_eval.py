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
import operator
import unittest
from tokenize import TokenInfo, Token

from csv_transformer import BINOP_BY_NAME, PREFIX_UNOP_BY_NAME, \
    INFIX_UNOP_BY_NAME
from csv_transformer.functions import to_date, to_datetime
from csv_transformer.simple_eval import *
import datetime as dt

from csv_transformer.simple_eval import ShuntingYard


class LiteralTestCase(unittest.TestCase):
    def test_repr(self):
        self.assertEqual("Literal('a')", repr(Literal("a")))

    def test_eq(self):
        self.assertEqual(Literal("a"), Literal("a"))
        self.assertNotEqual(Literal("a"), Literal("b"))
        self.assertNotEqual(Literal("a"), object())


class IdentifierTestCase(unittest.TestCase):
    def test_repr(self):
        self.assertEqual("Identifier('a')", repr(Identifier("a")))

    def test_eq(self):
        self.assertEqual(Identifier("a"), Identifier("a"))
        self.assertNotEqual(Identifier("a"), Identifier("b"))
        self.assertNotEqual(Identifier("a"), object())


def _f(x): return x


def _g(x): return x


class FunctionTestCase(unittest.TestCase):
    def test_repr(self):
        self.assertEqual("Function('f')", repr(Function("f", _f)))

    def test_eq(self):
        self.assertEqual(Function("f", _f), Function("f", _f))
        self.assertEqual(Function("f", _f), Function("f", _g))
        self.assertNotEqual(Function("f", _f), Function("g", _f))
        self.assertNotEqual(Function("f", _f), Function("g", _g))
        self.assertNotEqual(Function("f", _f), object())


class BinopTestCase(unittest.TestCase):
    def test_repr(self):
        self.assertEqual("BinOp('bof')", repr(BinOp("bof", True, True, _f)))

    def test_eq(self):
        self.assertEqual(BinOp("bof", True, True, _f),
                         BinOp("bof", True, True, _f))
        self.assertEqual(BinOp("bof", True, True, _f),
                         BinOp("bof", True, True, _g))
        self.assertNotEqual(BinOp("bof", True, True, _f),
                            BinOp("bog", True, True, _f))
        self.assertNotEqual(BinOp("bof", True, True, _f),
                            BinOp("bog", True, True, _g))
        self.assertNotEqual(BinOp("bof", True, True, _f), object())


class PrefixUnOpTestCase(unittest.TestCase):
    def test_repr(self):
        self.assertEqual("PrefixUnOp('bof')",
                         repr(PrefixUnOp("bof", True, True, _f)))

    def test_eq(self):
        self.assertEqual(PrefixUnOp("bof", True, True, _f),
                         PrefixUnOp("bof", True, True, _f))
        self.assertEqual(PrefixUnOp("bof", True, True, _f),
                         PrefixUnOp("bof", True, True, _g))
        self.assertNotEqual(PrefixUnOp("bof", True, True, _f),
                            PrefixUnOp("bog", True, True, _f))
        self.assertNotEqual(PrefixUnOp("bof", True, True, _f),
                            PrefixUnOp("bog", True, True, _g))
        self.assertNotEqual(PrefixUnOp("bof", True, True, _f), object())


class InfixUnOpTestCase(unittest.TestCase):
    def test_repr(self):
        self.assertEqual("InfixUnOp('bof')",
                         repr(InfixUnOp("bof", True, True, _f)))

    def test_eq(self):
        self.assertEqual(InfixUnOp("bof", True, True, _f),
                         InfixUnOp("bof", True, True, _f))
        self.assertEqual(InfixUnOp("bof", True, True, _f),
                         InfixUnOp("bof", True, True, _g))
        self.assertNotEqual(InfixUnOp("bof", True, True, _f),
                            InfixUnOp("bog", True, True, _f))
        self.assertNotEqual(InfixUnOp("bof", True, True, _f),
                            InfixUnOp("bog", True, True, _g))
        self.assertNotEqual(InfixUnOp("bof", True, True, _f), object())


class MiscTestCase(unittest.TestCase):
    def test_to_date(self):
        self.assertEqual(dt.date(2010, 5, 1), to_date("2010-05-01"))
        self.assertEqual(dt.date(2010, 5, 1), to_date(dt.date(2010, 5, 1)))
        self.assertEqual(dt.date(2010, 5, 1),
                         to_date(dt.datetime(2010, 5, 1, 15, 30, 45)))
        with self.assertRaises(ValueError):
            to_date("foo")
        with self.assertRaises(ValueError):
            to_date(1)

    def test_to_datetime(self):
        self.assertEqual(dt.datetime(2010, 5, 1, 12, 30, 15),
                         to_datetime("2010-05-01 12:30:15"))
        self.assertEqual(dt.datetime(2010, 5, 1, 0, 0),
                         to_datetime(dt.date(2010, 5, 1)))
        self.assertEqual(dt.datetime(2010, 5, 1, 15, 30, 45),
                         to_datetime(dt.datetime(2010, 5, 1, 15, 30, 45)))
        with self.assertRaises(ValueError):
            to_datetime("foo")
        with self.assertRaises(ValueError):
            to_datetime(1)


class TokenizeTestCase(unittest.TestCase):
    TOKEN_INFO_ENC = TokenInfo(ENCODING, 'utf-8', (0, 0), (0, 0), '')
    TOKEN_INFO_END_MARKER = TokenInfo(ENDMARKER, '', (2, 0), (2, 0), '')

    def test_tokenize_field(self):
        expr = 'it.year > 2000\n'
        self.assertEqual([
            self.TOKEN_INFO_ENC,
            TokenInfo(NAME, 'it', (1, 0), (1, 2), expr),
            TokenInfo(OP, '.', (1, 2), (1, 3), expr),
            TokenInfo(NAME, 'year', (1, 3), (1, 7), expr),
            TokenInfo(OP, '>', (1, 8), (1, 9), expr),
            TokenInfo(NUMBER, '2000', (1, 10), (1, 14), expr),
            TokenInfo(NEWLINE, '\n', (1, 14), (1, 15), expr),
            self.TOKEN_INFO_END_MARKER
        ], list(tokenize_expr(expr)))

    def test_tokenize_neg(self):
        expr = 'it.year > -2000\n'
        self.assertEqual([
            self.TOKEN_INFO_ENC,
            TokenInfo(NAME, 'it', (1, 0), (1, 2), expr),
            TokenInfo(OP, '.', (1, 2), (1, 3), expr),
            TokenInfo(NAME, 'year', (1, 3), (1, 7), expr),
            TokenInfo(OP, '>', (1, 8), (1, 9), expr),
            TokenInfo(OP, '-', (1, 10), (1, 11), expr),
            TokenInfo(NUMBER, '2000', (1, 11), (1, 15), expr),
            TokenInfo(NEWLINE, '\n', (1, 15), (1, 16), expr),
            self.TOKEN_INFO_END_MARKER
        ], list(tokenize_expr(expr)))

    def test_tokenize_function(self):
        expr = 'f(a, 2)\n'
        self.assertEqual([
            self.TOKEN_INFO_ENC,
            TokenInfo(NAME, 'f', (1, 0), (1, 1), expr),
            TokenInfo(OP, '(', (1, 1), (1, 2), expr),
            TokenInfo(NAME, 'a', (1, 2), (1, 3), expr),
            TokenInfo(OP, ',', (1, 3), (1, 4), expr),
            TokenInfo(NUMBER, '2', (1, 5), (1, 6), expr),
            TokenInfo(OP, ')', (1, 6), (1, 7), expr),
            TokenInfo(NEWLINE, '\n', (1, 7), (1, 8), expr),
            self.TOKEN_INFO_END_MARKER
        ], list(tokenize_expr(expr)))


class ShuntingYardTestCase(unittest.TestCase):
    def test_shunting_yard(self):
        tokens = tokenize_expr("year(it) > round(2000, 2)")
        self.assertEqual([
            'STOP',
            Identifier('it'),
            Function('year', None),
            'STOP',
            Literal(2000),
            Literal(2),
            Function('round', None),
            BinOp('>', -2, True, None)
        ], list(ShuntingYard(False, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
                             INFIX_UNOP_BY_NAME).process(tokens)))

    def test_shunting_yard2(self):
        tokens1 = tokenize_expr("min((a+2)*3, 4*2)")
        tokens = ShuntingYard(False, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
                              INFIX_UNOP_BY_NAME).process(tokens1)
        self.assertEqual([
            'STOP',
            Identifier('a'),
            Literal(2),
            BinOp('+', 0, True, operator.add),
            Literal(3),
            BinOp('*', 1, True, operator.mul),
            Literal(4),
            Literal(2),
            BinOp('*', 1, True, operator.mul),
            Function('min', min),
        ], tokens)

    def test_shunting_yard3(self):
        tokens = tokenize_expr(
            "str(day('2015-05-03'))+'/'+str(month('2015-05-03'))+'/'+str(year('2015-05-03'))")
        self.assertEqual(
            ['STOP',
             'STOP',
             Literal('2015-05-03'),
             Function('day', None),
             Function('str', None),
             Literal('/'),
             BinOp('+', 0, False, None),
             'STOP',
             'STOP',
             Literal('2015-05-03'),
             Function('month', None),
             Function('str', None),
             BinOp('+', 0, False, None),
             Literal('/'),
             BinOp('+', 0, False, None),
             'STOP',
             'STOP',
             Literal('2015-05-03'),
             Function('year', None),
             Function('str', None),
             BinOp('+', 0, False, None)
             ], ShuntingYard(False, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
                             INFIX_UNOP_BY_NAME).process(tokens))


class EvalTestCase(unittest.TestCase):
    def test_eval0(self):
        tokens1 = tokenize_expr("min((a+2)*3, 4*2)")
        tokens = ShuntingYard(False, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
                              INFIX_UNOP_BY_NAME).process(tokens1)
        value_by_name = {"a": 10}
        self.assertEqual(8, evaluate(tokens, value_by_name))

    def test_eval_expr_assoc(self):
        self.assertEqual(2, eval_expr("5-2-1"))
        self.assertEqual(4, eval_expr("16/2/2"))

    def test_eval_expr_unary(self):
        self.assertEqual(-4, eval_expr("2*(-2)"))
        self.assertEqual(4, eval_expr("2 + -round(-2.5)", debug=True))
        self.assertEqual(-2, eval_expr("2 + -round(1.3+2.5)", debug=True))
        self.assertEqual(-4, eval_expr("-2*2"))
        self.assertEqual(-4, eval_expr("2*-2"))
        self.assertEqual(0, eval_expr("-2+2"))
        self.assertEqual(2, eval_expr("--2"))
        self.assertEqual(-2, eval_expr("-(-2+4)"))
        self.assertEqual(1, eval_expr("2--2 - 3", debug=True))
        self.assertEqual("2-8", eval_expr("format('{}{}', -round(-2.5), -2*4)",
                                          debug=True))
        self.assertEqual("2.5-8",
                         eval_expr("format('{}{}', -(-2.5), -2*4)", debug=True))

    def test_eval_expr(self):
        self.assertEqual(10, eval_expr("5*2"))
        self.assertEqual(3, eval_expr("5-2"))
        self.assertEqual(2, eval_expr("min(5, 2, 7, min(3, 8))"))
        self.assertEqual(4.0, eval_expr("avg(5, 2, 5)"))
        self.assertEqual("03/05/2015",
                         eval_expr("'03'+'/'+'05'+'/'+'2015'"))

    def test_eval_date(self):
        self.assertEqual(2015, eval_expr("year(x)", {'x': '2015-05-03'}))
        self.assertEqual(2015, eval_expr("year(\"2015-05-03\")"))
        self.assertEqual(2015, eval_expr("year('2015-05-03')"))
        self.assertEqual(3, eval_expr("day('2015-05-03')"))
        self.assertEqual(5, eval_expr("month('2015-05-03')"))
        self.assertEqual(2015, eval_expr("year('2015-05-03')"))
        self.assertEqual(12, eval_expr("hour('2015-05-03 12:30:15')"))
        self.assertEqual(30, eval_expr("minute('2015-05-03 12:30:15')"))
        self.assertEqual(15, eval_expr("second('2015-05-03 12:30:15')"))

    def test_eval_str(self):
        self.assertEqual("FOO", eval_expr("upper('foo')"))
        self.assertEqual("foo", eval_expr("trim(' foo  ')"))

    def test_eval_2(self):
        self.assertEqual("3/5/2015", eval_expr(
            "str(day('2015-05-03'))+'/'+str(month('2015-05-03'))+'/'+str(year('2015-05-03'))"))
        self.assertEqual("03/05/2015", eval_expr(
            "format('{:02}/{:02}/{:04}', day('2015-05-03'), month('2015-05-03'), year('2015-05-03'))"))
        self.assertEqual(10, eval_expr("(2+3)*2"))
        self.assertEqual(10, eval_expr("(2+x)*2", {"x": 3}))
        self.assertEqual(10, eval_expr("2*(2+x)", {"x": 3}))
        self.assertEqual(20, eval_expr("2*(2+x)*2", {"x": 3}))

    def test_eval_if(self):
        self.assertEqual(1, eval_expr("if(x > 2, 2, x)", {"x": 1}))
        self.assertEqual(2, eval_expr("if(x > 2, 2, x)", {"x": 2}))
        self.assertEqual(2, eval_expr("if(x > 2, 2, x)", {"x": 3}))
        self.assertEqual(2, eval_expr("if(x > 2, 2, x)", {"x": 4}))

    def test_eval_case(self):
        self.assertEqual(-2,
                         eval_expr("case(x > 2, 2, x < -2, -2, x)", {"x": -4}))
        self.assertEqual(1,
                         eval_expr("case(x > 2, 2, x < -2, -2, x)", {"x": 1}))
        self.assertEqual(2,
                         eval_expr("case(x > 2, 2, x < -2, -2, x)", {"x": 4}))

    def test_eval_add_date(self):
        self.assertEqual(dt.date(2016, 1, 12),
                         eval_expr("add_years(date('2014-01-12'), 2)"))
        self.assertEqual(dt.date(2014, 3, 12),
                         eval_expr("add_months(date('2014-01-12'), 2)"))
        self.assertEqual(dt.date(2014, 2, 11),
                         eval_expr("add_days(date('2014-01-12'), 30)"))
        self.assertEqual(dt.datetime(2014, 1, 13, 6, 0),
                         eval_expr("add_hours(date('2014-01-12'), 30)"))
        self.assertEqual(dt.datetime(2014, 1, 12, 0, 30),
                         eval_expr("add_minutes(date('2014-01-12'), 30)"))
        self.assertEqual(dt.datetime(2014, 1, 12, 0, 0, 30),
                         eval_expr("add_seconds(date('2014-01-12'), 30)"))

    def test_eval_add_datetime(self):
        self.assertEqual(dt.datetime(2016, 1, 12, 12, 30, 21), eval_expr(
            "add_years(datetime('2014-01-12 12:30:21'), 2)"))
        self.assertEqual(dt.datetime(2014, 3, 12, 12, 30, 21), eval_expr(
            "add_months(datetime('2014-01-12 12:30:21'), 2)"))
        self.assertEqual(dt.datetime(2014, 2, 11, 12, 30, 21), eval_expr(
            "add_days(datetime('2014-01-12 12:30:21'), 30)"))
        self.assertEqual(dt.datetime(2014, 1, 13, 18, 30, 21), eval_expr(
            "add_hours(datetime('2014-01-12 12:30:21'), 30)"))
        self.assertEqual(dt.datetime(2014, 1, 12, 13, 0, 21), eval_expr(
            "add_minutes(datetime('2014-01-12 12:30:21'), 30)"))
        self.assertEqual(dt.datetime(2014, 1, 12, 12, 30, 51), eval_expr(
            "add_seconds(datetime('2014-01-12 12:30:21'), 30)"))

    def test_eval_age(self):
        # 2013-01-13 + 1 year = 2014-01-13
        # 2014-01-13 + 0 month = 2014-01-13
        self.assertEqual(dt.date(2014, 1, 13) + dt.timedelta(days=30),
                         dt.date(2014, 2, 12))
        self.assertEqual((1, 0, 30), eval_expr(
            "age(date('2014-02-12'), date('2013-01-13'))"))

        # 1975-04-20 + 38 years = 2013-04-20
        # 2013-04-20 + 8 months = 2013-12-20
        self.assertEqual(dt.date(2013, 12, 20) + dt.timedelta(days=23),
                         dt.date(2014, 1, 12))
        self.assertEqual((38, 8, 23), eval_expr(
            "age(datetime('2014-01-12 12:30:21'), datetime('1975-04-20 13:30:00'))"))

    def test_precedence(self):
        self.assertEqual(13, eval_expr("3+5*2"))
        self.assertEqual(13, eval_expr("5*2+3"))

    def test_err_eval(self):
        with self.assertRaises(ValueError):
            eval_expr(",")

    def test_err_comma(self):
        with self.assertRaises(ValueError):
            eval_expr("1+2.5,")

    def test_err_token(self):
        with self.assertRaises(ValueError):
            ShuntingYard(False, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
                         INFIX_UNOP_BY_NAME).process(
                iter([TokenInfo(ENCODING, "", 1, 1, 1), TokenInfo(ENCODING, "", 1, 1, 1)]))


def eval_expr(s: str, value_by_name: Optional[Mapping[str, Any]] = None,
              debug=False) -> Any:
    tokens = tokenize_expr(s)
    if debug:
        tokens = list(tokens)
        print("Ts: {}".format(tokens))
        tokens = iter(tokens)
    tokens = ShuntingYard(debug, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
                          INFIX_UNOP_BY_NAME).process(tokens)
    return evaluate(tokens, value_by_name)


if __name__ == '__main__':
    unittest.main()
