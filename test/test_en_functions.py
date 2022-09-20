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

from csv_transformer.en_functions import (
    FUNC_BY_TYPE)

import datetime as dt


class EnFunctionsTestCase(unittest.TestCase):
    def test_date(self):
        self.assertEqual(FUNC_BY_TYPE["datetime"]("12/05/2017 12:30:15"),
                         dt.datetime(2017, 5, 12, 12, 30, 15))
        self.assertEqual(FUNC_BY_TYPE["datetime"]("2017-05-12 12:30:15"),
                         dt.datetime(2017, 5, 12, 12, 30, 15))
        self.assertEqual(FUNC_BY_TYPE["datetime_us"]("2017-05-12 12:30:15"),
                         dt.datetime(2017, 5, 12, 12, 30, 15))

    def test_datetime(self):
        self.assertEqual(FUNC_BY_TYPE["date"]("12/05/2017"),
                         dt.date(2017, 5, 12))
        self.assertEqual(FUNC_BY_TYPE["date"]("2017-05-12"),
                         dt.date(2017, 5, 12))
        self.assertEqual(FUNC_BY_TYPE["date_us"]("2017-05-12"),
                         dt.date(2017, 5, 12))

    def test_float(self):
        import locale
        cur = locale.getlocale(locale.LC_NUMERIC)
        locale.setlocale(locale.LC_NUMERIC, ('fr_FR', 'UTF-8'))
        try:
            self.assertEqual(FUNC_BY_TYPE["float"]("1 235,7"), 1235.7)
            self.assertEqual(FUNC_BY_TYPE["float_us"]("1235.7"), 1235.7)
        finally:
            locale.setlocale(locale.LC_NUMERIC, cur)


if __name__ == '__main__':
    unittest.main()
