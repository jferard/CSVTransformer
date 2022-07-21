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

import unittest
from tokenize import TokenInfo

from csv_transformer.simple_eval import *
import datetime as dt


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
        self.assertEqual(BinOp("bof", True, True, _f), BinOp("bof", True, True, _f))
        self.assertEqual(BinOp("bof", True, True, _f), BinOp("bof", True, True, _g))
        self.assertNotEqual(BinOp("bof", True, True, _f), BinOp("bog", True, True, _f))
        self.assertNotEqual(BinOp("bof", True, True, _f), BinOp("bog", True, True, _g))
        self.assertNotEqual(BinOp("bof", True, True, _f), object())


class PrefixUnOpTestCase(unittest.TestCase):
    def test_repr(self):
        self.assertEqual("PrefixUnOp('bof')", repr(PrefixUnOp("bof", True, True, _f)))

    def test_eq(self):
        self.assertEqual(PrefixUnOp("bof", True, True, _f), PrefixUnOp("bof", True, True, _f))
        self.assertEqual(PrefixUnOp("bof", True, True, _f), PrefixUnOp("bof", True, True, _g))
        self.assertNotEqual(PrefixUnOp("bof", True, True, _f), PrefixUnOp("bog", True, True, _f))
        self.assertNotEqual(PrefixUnOp("bof", True, True, _f), PrefixUnOp("bog", True, True, _g))
        self.assertNotEqual(PrefixUnOp("bof", True, True, _f), object())


class InfixUnOpTestCase(unittest.TestCase):
    def test_repr(self):
        self.assertEqual("InfixUnOp('bof')", repr(InfixUnOp("bof", True, True, _f)))

    def test_eq(self):
        self.assertEqual(InfixUnOp("bof", True, True, _f), InfixUnOp("bof", True, True, _f))
        self.assertEqual(InfixUnOp("bof", True, True, _f), InfixUnOp("bof", True, True, _g))
        self.assertNotEqual(InfixUnOp("bof", True, True, _f), InfixUnOp("bog", True, True, _f))
        self.assertNotEqual(InfixUnOp("bof", True, True, _f), InfixUnOp("bog", True, True, _g))
        self.assertNotEqual(InfixUnOp("bof", True, True, _f), object())


class MiscTestCase(unittest.TestCase):
    def test_to_date(self):
        self.assertEqual(dt.date(2010, 5, 1), to_date("2010-05-01"))
        self.assertEqual(dt.date(2010, 5, 1), to_date(dt.date(2010, 5, 1)))
        self.assertEqual(dt.date(2010, 5, 1), to_date(dt.datetime(2010, 5, 1, 15, 30, 45)))
        with self.assertRaises(ValueError):
            to_date("foo")
        with self.assertRaises(ValueError):
            to_date(1)


class TokenizeTestCase(unittest.TestCase):
    TOKEN_INFO_ENC = TokenInfo(ENCODING, 'utf-8', (0, 0), (0, 0), '')
    TOKEN_INFO_END_MARKER = TokenInfo(ENDMARKER, '', (2, 0), (2, 0), '')

    def test_tokenize_field(self):
        self.assertEqual([
            self.TOKEN_INFO_ENC,
            TokenInfo(NAME, 'it', (1, 0), (1, 2), 'it.year > 2000'),
            TokenInfo(OP, '.', (1, 2), (1, 3), 'it.year > 2000'),
            TokenInfo(NAME, 'year', (1, 3), (1, 7), 'it.year > 2000'),
            TokenInfo(OP, '>', (1, 8), (1, 9), 'it.year > 2000'),
            TokenInfo(NUMBER, '2000', (1, 10), (1, 14), 'it.year > 2000'),
            TokenInfo(NEWLINE, '', (1, 14), (1, 15), ''),
            self.TOKEN_INFO_END_MARKER
        ], list(tokenize_expr("it.year > 2000")))

    def test_tokenize_neg(self):
        self.assertEqual([
            self.TOKEN_INFO_ENC,
            TokenInfo(NAME, 'it', (1, 0), (1, 2), 'it.year > -2000'),
            TokenInfo(OP, '.', (1, 2), (1, 3), 'it.year > -2000'),
            TokenInfo(NAME, 'year', (1, 3), (1, 7), 'it.year > -2000'),
            TokenInfo(OP, '>', (1, 8), (1, 9), 'it.year > -2000'),
            TokenInfo(OP, '-', (1, 10), (1, 11), 'it.year > -2000'),
            TokenInfo(NUMBER, '2000', (1, 11), (1, 15), 'it.year > -2000'),
            TokenInfo(NEWLINE, '', (1, 15), (1, 16), ''),
            self.TOKEN_INFO_END_MARKER
        ], list(tokenize_expr("it.year > -2000")))

    def test_tokenize_function(self):
        self.assertEqual([
            self.TOKEN_INFO_ENC,
            TokenInfo(NAME, 'f', (1, 0), (1, 1), 'f(a, 2)'),
            TokenInfo(OP, '(', (1, 1), (1, 2), 'f(a, 2)'),
            TokenInfo(NAME, 'a', (1, 2), (1, 3), 'f(a, 2)'),
            TokenInfo(OP, ',', (1, 3), (1, 4), 'f(a, 2)'),
            TokenInfo(NUMBER, '2', (1, 5), (1, 6), 'f(a, 2)'),
            TokenInfo(OP, ')', (1, 6), (1, 7), 'f(a, 2)'),
            TokenInfo(NEWLINE, '', (1, 7), (1, 8), ''),
            self.TOKEN_INFO_END_MARKER
        ], list(tokenize_expr("f(a, 2)")))


class ShuntingYardTestCase(unittest.TestCase):
    def test_shunting_yard(self):
        self.assertEqual([
            'STOP',
            Identifier('it'),
            Function('year', None),
            'STOP',
            Literal(2000),
            Literal(2),
            Function('round', None),
            BinOp('>', -2, True, None)
        ], list(shunting_yard(tokenize_expr("year(it) > round(2000, 2)"))))

    def test_shunting_yard2(self):
        tokens = shunting_yard(tokenize_expr("min((a+2)*3, 4*2)"))
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
             ], shunting_yard(tokenize_expr(
                "str(day('2015-05-03'))+'/'+str(month('2015-05-03'))+'/'+str(year('2015-05-03'))")))


class EvalTestCase(unittest.TestCase):
    def test_eval0(self):
        tokens = shunting_yard(tokenize_expr("min((a+2)*3, 4*2)"))
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
        self.assertEqual("2-8", eval_expr("format('{}{}', -round(-2.5), -2*4)", debug=True))
        self.assertEqual("2.5-8", eval_expr("format('{}{}', -(-2.5), -2*4)", debug=True))

    def test_eval_expr(self):
        self.assertEqual(10, eval_expr("5*2"))
        self.assertEqual(3, eval_expr("5-2"))
        self.assertEqual(2, eval_expr("min(5, 2, 7, min(3, 8))"))
        self.assertEqual(4.0, eval_expr("avg(5, 2, 5)"))
        self.assertEqual(2015, eval_expr("year(x)", {'x': '2015-05-03'}))
        self.assertEqual(2015, eval_expr("year(\"2015-05-03\")"))
        self.assertEqual(2015, eval_expr("year('2015-05-03')"))
        self.assertEqual("3", eval_expr("str(day('2015-05-03'))"))
        self.assertEqual("5", eval_expr("str(month('2015-05-03'))"))
        self.assertEqual("2015", eval_expr("str(year('2015-05-03'))"))
        self.assertEqual("03/05/2015",
                         eval_expr("'03'+'/'+'05'+'/'+'2015'"))

    def test_eval_2(self):
        self.assertEqual("3/5/2015", eval_expr(
            "str(day('2015-05-03'))+'/'+str(month('2015-05-03'))+'/'+str(year('2015-05-03'))"))
        self.assertEqual("03/05/2015", eval_expr(
            "format('{:02}/{:02}/{:04}', day('2015-05-03'), month('2015-05-03'), year('2015-05-03'))"))
        self.assertEqual(10, eval_expr("(2+3)*2"))
        self.assertEqual(10, eval_expr("(2+x)*2", {"x": 3}))
        self.assertEqual(10, eval_expr("2*(2+x)", {"x": 3}))
        self.assertEqual(20, eval_expr("2*(2+x)*2", {"x": 3}))

    def test_precedence(self):
        self.assertEqual(13, eval_expr("3+5*2"))
        self.assertEqual(13, eval_expr("5*2+3"))

    def test_err_eval(self):
        with self.assertRaises(ValueError):
            eval_expr(",")

    def test_err_comma(self):
        with self.assertRaises(ValueError):
            eval_expr("1+2.5,")


if __name__ == '__main__':
    unittest.main()
