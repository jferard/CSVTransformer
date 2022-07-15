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

from simple_eval import *
from simple_eval import evaluate


class TokenizeTestCase(unittest.TestCase):
    def test_tokenize(self):
        self.assertEqual([TokenInfo(type=ENCODING, string='utf-8',
                                    start=(0, 0), end=(0, 0), line=''),
                          TokenInfo(type=NAME, string='it', start=(1, 0),
                                    end=(1, 2), line='it.year > 2000'),
                          TokenInfo(type=OP, string='.', start=(1, 2),
                                    end=(1, 3), line='it.year > 2000'),
                          TokenInfo(type=NAME, string='year', start=(1, 3),
                                    end=(1, 7), line='it.year > 2000'),
                          TokenInfo(type=OP, string='>', start=(1, 8),
                                    end=(1, 9), line='it.year > 2000'),
                          TokenInfo(type=NUMBER, string='2000',
                                    start=(1, 10), end=(1, 14),
                                    line='it.year > 2000'),
                          TokenInfo(type=NEWLINE, string='', start=(1, 14),
                                    end=(1, 15), line=''),
                          TokenInfo(type=ENDMARKER, string='', start=(2, 0),
                                    end=(2, 0), line='')],
                         list(tokenize_expr("it.year > 2000")))

    def test_tokenize2(self):
        self.assertEqual([
            TokenInfo(type=ENCODING, string='utf-8', start=(0, 0),
                      end=(0, 0), line=''),
            TokenInfo(type=NAME, string='f', start=(1, 0), end=(1, 1),
                      line='f(a, 2)'),
            TokenInfo(type=OP, string='(', start=(1, 1), end=(1, 2),
                      line='f(a, 2)'),
            TokenInfo(type=NAME, string='a', start=(1, 2), end=(1, 3),
                      line='f(a, 2)'),
            TokenInfo(type=OP, string=',', start=(1, 3), end=(1, 4),
                      line='f(a, 2)'),
            TokenInfo(type=NUMBER, string='2', start=(1, 5), end=(1, 6),
                      line='f(a, 2)'),
            TokenInfo(type=OP, string=')', start=(1, 6), end=(1, 7),
                      line='f(a, 2)'),
            TokenInfo(type=NEWLINE, string='', start=(1, 7), end=(1, 8),
                      line=''),
            TokenInfo(type=ENDMARKER, string='', start=(2, 0), end=(2, 0),
                      line='')
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


if __name__ == '__main__':
    unittest.main()
