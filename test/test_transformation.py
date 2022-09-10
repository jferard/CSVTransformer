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

    def test_type_map(self):
        csv_in_string = "a,b\n1,2\n3,4"
        csv_out_string = "a,b\r\n2,2\r\n6,4\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "type": "int",
                    "map": "it*2"
                }
            }
        }, csv_in_string, csv_out_string)

    def test_type2_map(self):
        csv_in_string = "a,b\n1,2\n3,4"
        csv_out_string = "a,b\r\n2.0,2\r\n6.0,4\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "type": "float(it)",
                    "map": "it*2"
                }
            }
        }, csv_in_string, csv_out_string)

    def test_main_filter(self):
        csv_in_string = "a,b\n1,2\n3,2"
        csv_out_string = "a,b\r\n3,2\r\n"

        self._test_transformation({
            "filter": "a > b"
        }, csv_in_string, csv_out_string)

    def test_string_agg(self):
        csv_in_string = "a,b\n1,2\n1,3"
        csv_out_string = "a,b\r\n1,\"2.0, 3.0\"\r\n"

        self._test_transformation({
            "cols": {
                "b": {
                    "type": "float(it)",
                    "agg": "string_agg"
                }
            }
        }, csv_in_string, csv_out_string)

    def test_duplicate(self):
        csv_in_string = "a,a\n1,2\n1,3"
        csv_out_string = "a_1,a_2\r\n1,\"2.0, 3.0\"\r\n"

        self._test_transformation({
            "cols": {
                "a_2": {
                    "type": "float(it)",
                    "agg": "string_agg"
                }
            }
        }, csv_in_string, csv_out_string)

    def test_string_sum(self):
        csv_in_string = "a,b\n1,2\n1,3"
        csv_out_string = "a,b\r\n1,5.0\r\n"

        self._test_transformation({
            "cols": {
                "b": {
                    "type": "float(it)",
                    "agg": "sum"
                }
            }
        }, csv_in_string, csv_out_string)

    def test_normalize(self):
        csv_in_string = "À demain, été comme hiver\n1,2\n1,3"
        csv_out_string = "a_demain,ete_comme_hiver\r\n1,5.0\r\n"

        self._test_transformation({
            "default_col": {"normalize": True},
            "cols": {
                "ete_comme_hiver": {
                    "type": "float(it)",
                    "agg": "sum"
                }
            }
        }, csv_in_string, csv_out_string)

    def test_agg(self):
        csv_in_string = "a,b\n1,2\n1,3"
        csv_out_string = "b\r\n5.0\r\n"

        self._test_transformation({
            "default_col": {"visible": False},
            "cols": {
                "b": {
                    "visible": True,
                    "type": "float(it)",
                    "agg": "sum"
                }
            }
        }, csv_in_string, csv_out_string)

    def test_extra(self):
        csv_in_string = "a\n1,2,3\n4,5,6"
        csv_out_string = "a,ex_1,ex_2,ex_3\r\n5.0,7.0,9.0,\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "type": "float(it)",
                    "agg": "sum"
                },
                "ex_1": {
                    "type": "float(it)",
                    "agg": "sum"
                },
                "ex_2": {
                    "type": "float(it)",
                    "agg": "sum"
                }
            },
            "extra": {"prefix": "ex", "count": 4}
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
        csv_in = {"encoding": "utf-8", "path": csv_in_path,
                  "skipinitialspace": True}
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
        ct = transformation._col_transformation_by_name['a']
        with self.assertRaises(KeyError):
            ct.type_value("2")

    def test_err_col_agg(self):
        transformation_dict = {
            "cols": {
                "a": {
                    "agg": "FOO"
                }
            }
        }
        transformation = TransformationParser(False).parse(transformation_dict)
        self.assertFalse(transformation.has_agg())


class CSVTransformerIntegrationTestCase(unittest.TestCase):
    def test_transform1(self):
        csv_in = {"encoding": "utf-8", "path": (
                FIXTURE_PATH / "StockEtablissementLiensSuccession_utf8.csv")}
        csv_out = {"path": FIXTURE_PATH / "test.csv"}
        transformation_dict = {
            "filter": "date_lien_succ > date('2000-01-01')",
            "default_col": {"visible": False},
            "cols": {
                "siretEtablissementSuccesseur": {
                    "visible": True,
                    "type": "int",
                    "agg": "mean",
                    "rename": "Avg siretSuccesseur"
                },
                "dateLienSuccession": {
                    "visible": True,
                    "type": "date",
                    "id": "date_lien_succ",
                },
            }
        }
        main(csv_in, transformation_dict, csv_out)

    def test_transform2(self):
        csv_in = {"encoding": "latin-1", "path": (
                FIXTURE_PATH / "resultats-par-niveau-cirlg-t1-france-entiere.txt"),
                  "delimiter": ";"
                  }
        csv_out = {"path": FIXTURE_PATH / "test.csv"}
        transformation_dict = {
            "default_col": {"visible": False},
            "cols": {
                "Code de la circonscription": {
                    "visible": True,
                    "agg": "count",
                    "rename": "Nombre de circonscriptions"
                }, "Inscrits": {
                    "visible": True,
                    "type": "int",
                    "agg": "sum"
                },
                "Abstentions": {
                    "visible": True,
                    "type": "int",
                    "agg": "sum"
                },
                "Votants": {
                    "visible": True,
                    "type": "int",
                    "agg": "sum"
                },
                "Blancs": {
                    "visible": True,
                    "type": "int",
                    "agg": "sum"
                },
                "Nuls": {
                    "visible": True,
                    "type": "int",
                    "agg": "sum"
                },
                "Exprimés": {
                    "visible": True,
                    "type": "int",
                    "agg": "sum"
                },
            }
        }
        main(csv_in, transformation_dict, csv_out)


class ExpressionParserTestCase(unittest.TestCase):
    def test_func(self):
        f = ExpressionParser().parse("it * 2")
        self.assertEqual(6, f(3))


class HeaderTestCase(unittest.TestCase):
    def test_improve(self):
        self.assertEqual(["a_1", "a_2", "a_3"], improve_header(["a", "a", "a"]))
        self.assertEqual(["a_2", "a_1", "a_3"],
                         improve_header(["a", "a_1", "a"]))
        self.assertEqual(["a_1", "_1", "b", "_2", "a_2", "_3"],
                         improve_header(["a", "", "b", "", "a", ""]))

    def test_add_fields(self):
        self.assertEqual(
            ['a', 'b', 'c', 'extra_1', 'extra_2', 'extra_3', 'extra_4',
             'extra_5'], add_fields(["a", "b", "c"], total=8))
        self.assertEqual(["a", "b", "c"], add_fields(["a", "b", "c"], total=2))

    def test_normalize(self):
        self.assertEqual("un_test_effectue", normalize("Un Test  Effectué"))


if __name__ == '__main__':
    unittest.main()
