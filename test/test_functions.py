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
from decimal import Decimal
from unittest import mock
from unittest.mock import Mock

from csv_transformer.functions import age, str_to_decimal
import datetime as dt


class FunctionsTestCase(unittest.TestCase):
    def test_age(self):
        self.assertEqual((0, 9, 10),
                         age(dt.date(2020, 10, 11), dt.date(2020, 1, 1)))

    def test_age_value_error(self):
        with self.assertRaises(ValueError):
            age(dt.date(2020, 1, 1), dt.date(2020, 10, 11))

    @mock.patch("csv_transformer.functions.dt")
    def test_age_now(self, patched_dt):
        patched_dt.datetime = Mock()
        patched_dt.datetime.now.return_value = dt.datetime(2022, 9, 24)

        self.assertEqual((1, 11, 13),
                         age(dt.date(2020, 10, 11)))

    def test_str_to_decimal(self):
        self.assertEqual(Decimal("1005.56"), str_to_decimal("1005.56"))
        self.assertEqual(Decimal("1005.56"), str_to_decimal("1 005,56"))


if __name__ == '__main__':
    unittest.main()
