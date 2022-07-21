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
import io
import unittest
from pathlib import Path
from unittest import mock

FIXTURE_PATH = Path(__file__).parent / "fixture"

from csv_transformer.transformation import *


class CSVTransformerTestCase(unittest.TestCase):
    def test_transform1(self):
        csv_in = {"encoding": "utf-8", "path": (
                FIXTURE_PATH / "StockEtablissementLiensSuccession_utf8.csv")}
        csv_out = {"path": FIXTURE_PATH / "test.csv"}
        transformation_dict = {
            "filter": "date_lien_succ > date('2000-01-01')",
            "cols": {
                "siretEtablissementPredecesseur": {
                    "visible": False
                },
                "siretEtablissementSuccesseur": {
                    "type": "int",
                    "agg": "avg",
                    "rename": "Avg siretSuccesseur"
                },
                "dateLienSuccession": {
                    "type": "date",
                    "id": "date_lien_succ",
                },
                "transfertSiege": {
                    "visible": False
                },
                "continuiteEconomique": {
                    "visible": False
                },
                "dateDernierTraitementLienSuccession": {
                    "visible": False
                }
            }
        }
        main(csv_in, transformation_dict, csv_out)

    def test_transform_without_agg(self):
        csv_in_string = "a\n1"
        csv_out_string = "A\r\n1\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "visible": True,
                    "rename": "A"
                },
            }
        }, csv_in_string, csv_out_string)

    def test_transform_without_agg_and_filter(self):
        csv_in_string = "a\n1\n3"
        csv_out_string = "A\r\n3\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "visible": True,
                    "rename": "A",
                    "filter": "int(it) > 2"
                }
            }
        }, csv_in_string, csv_out_string)

    def test_main_filter(self):
        csv_in_string = "a,b\n1,2\n3,2"
        csv_out_string = "a,b\r\n3,2\r\n"

        self._test_transformation({
            "filter": "a > b"
        }, csv_in_string, csv_out_string)

    def _test_transformation(self, transformation_dict, csv_in_string,
                             csv_out_string):
        self._test_regular_transformation(transformation_dict, csv_in_string,
                                          csv_out_string)
        self._test_risky_transformation(transformation_dict, csv_in_string,
                                        csv_out_string)

    def _test_regular_transformation(self, transformation_dict, csv_in_string,
                                     csv_out_string):
        csv_in_path = mock.Mock()
        csv_in_path.open.side_effect = [io.StringIO(csv_in_string)]
        csv_out_path = mock.Mock()
        csv_out_file = io.StringIO()
        csv_out_file.close = lambda: None
        csv_out_path.open.side_effect = [csv_out_file]
        csv_out_path.close.side_effect = []
        csv_in = {"encoding": "utf-8", "path": csv_in_path}
        csv_out = {"path": csv_out_path}
        transformation_dict = transformation_dict
        main(csv_in, transformation_dict, csv_out)
        self.assertEqual(csv_out_string, csv_out_file.getvalue())

    def _test_risky_transformation(self, transformation_dict, csv_in_string,
                                   csv_out_string):
        csv_in_path = mock.Mock()
        csv_in_path.open.side_effect = [io.StringIO(csv_in_string)]
        csv_out_path = mock.Mock()
        csv_out_file = io.StringIO()
        csv_out_file.close = lambda: None
        csv_out_path.open.side_effect = [csv_out_file]
        csv_out_path.close.side_effect = []
        csv_in = {"encoding": "utf-8", "path": csv_in_path}
        csv_out = {"path": csv_out_path}
        transformation_dict = transformation_dict
        main(csv_in, transformation_dict, csv_out, True)
        self.assertEqual(csv_out_string, csv_out_file.getvalue())


class CSVTransformerParserTestCase(unittest.TestCase):
    def test_err_col_type(self):
        transformation_dict = {
            "cols": {
                "a": {
                    "type": "INT"
                }
            }
        }
        transformation = TransformationParser(False).parse(transformation_dict)
        self.assertEqual({}, transformation._col_type_by_name)

    def test_err_col_agg(self):
        transformation_dict = {
            "cols": {
                "a": {
                    "agg": "FOO"
                }
            }
        }
        transformation = TransformationParser(False).parse(transformation_dict)
        self.assertEqual({}, transformation._col_agg_by_name)


if __name__ == '__main__':
    unittest.main()
