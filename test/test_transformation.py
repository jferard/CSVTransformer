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

from csv_transformer import main, TransformationJsonParser

FIXTURE_PATH = Path(__file__).parent / "fixture"

from csv_transformer.transformation import *
import datetime as dt

from csv_transformer.en_functions import (
    FUNC_BY_TYPE, FUNC_BY_AGG, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
    INFIX_UNOP_BY_NAME)


class CSVTransformerTestCase(unittest.TestCase):
    def _test_transformation(self, transformation_dict, csv_in_string,
                             csv_out_string):
        self._test_regular_transformation(transformation_dict, csv_in_string,
                                          csv_out_string)
        self._test_risky_transformation(transformation_dict, csv_in_string,
                                        csv_out_string)

    def _test_regular_transformation(self, transformation_dict, csv_in_string,
                                     csv_out_string):
        ret = self._apply_regular_transformation(transformation_dict,
                                                 csv_in_string)
        self.assertEqual(csv_out_string, ret)

    def _apply_regular_transformation(self, transformation_dict, csv_in_string):
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
        ret = csv_out_file.getvalue()
        return ret

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


class CSVTransformerWithoutAggTestCase(CSVTransformerTestCase):
    def test_void_trans(self):
        csv_in_string = "a,b\n1,2\n3,2\n1,1"
        csv_out_string = "a,b\r\n1,2\r\n3,2\r\n1,1\r\n"

        self._test_transformation({
        }, csv_in_string, csv_out_string)

    def test_entity_filter(self):
        csv_in_string = "a,b\n1,2\n3,2\n1,1"
        csv_out_string = "a,b\r\n3,2\r\n1,1\r\n"

        self._test_transformation({
            "entity_filter": "a >= b"
        }, csv_in_string, csv_out_string)

    def test_entity_filter_ids1(self):
        csv_in_string = "a,b\n1,2\n3,2\n1,1"
        csv_out_string = "a,b\r\n3,2\r\n1,1\r\n"

        self._test_transformation({
            "entity_filter": "Y >= Z",
            "cols": {
                "a": {"id": "Y"},
                "b": {"id": "Z"},
            }
        }, csv_in_string, csv_out_string)

    def test_entity_filter_ids2(self):
        csv_in_string = "a,b\n1,2\n3,2\n1,1"
        csv_out_string = "a,b\r\n1,2\r\n"

        self._test_transformation({
            "entity_filter": "Y < Z",
            "cols": {
                "a": {"id": "Y"},
                "b": {"id": "Z"},
            }
        }, csv_in_string, csv_out_string)

    def test_default_normalize(self):
        csv_in_string = "À demain, été comme hiver\n1,2\n1,3"
        csv_out_string = "a_demain,ete_comme_hiver\r\n1,2\r\n1,3\r\n"

        self._test_transformation({
            "default_col": {"normalize": True},
        }, csv_in_string, csv_out_string)

    def test_default_normalize_rename(self):
        csv_in_string = "À demain, été comme hiver\n1,2\n1,3"
        csv_out_string = "A_DEMAIN,ete_comme_hiver\r\n1,2\r\n1,3\r\n"

        self._test_transformation({
            "default_col": {"normalize": True},
            "cols": {"À demain": {"rename": "A_DEMAIN"}}
        }, csv_in_string, csv_out_string)

    def test_default_visible_false(self):
        csv_in_string = "a,b,c\n1,2,3\n3,2,1\n1,1,1"
        csv_out_string = "b\r\n2\r\n2\r\n1\r\n"

        self._test_transformation({
            "default_col": {"visible": False},
            "cols": {
                "b": {"visible": True},
            }
        }, csv_in_string, csv_out_string)

    def test_visible_false(self):
        csv_in_string = "a,b,c\n1,2,3\n3,2,1\n1,1,1"
        csv_out_string = "a,c\r\n1,3\r\n3,1\r\n1,1\r\n"

        self._test_transformation({
            "cols": {
                "b": {"visible": False},
            }
        }, csv_in_string, csv_out_string)

    def test_type1(self):
        csv_in_string = "a,b\n1,2\n3,4"
        csv_out_string = "a,b\r\n1.0,2\r\n3.0,4\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "type": "float",
                }
            }
        }, csv_in_string, csv_out_string)

    def test_type2(self):
        csv_in_string = "a\n03/01/2012\n03/06/2003"
        csv_out_string = "a\r\n2012-01-03\r\n2003-06-03\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "type": "date",
                }
            }
        }, csv_in_string, csv_out_string)

    def test_type3(self):
        csv_in_string = "a\n03.01.2012\n03.06.2003"
        csv_out_string = "a\r\n2012-01-03\r\n2003-06-03\r\n"

        self._test_regular_transformation({
            "cols": {
                "a": {
                    "type": "strpdate(it, '%d.%m.%Y')",
                }
            }
        }, csv_in_string, csv_out_string)

    def test_filter(self):
        csv_in_string = "a,b\n2,2\n3,2\n1,1"
        csv_out_string = "a,b\r\n3,2\r\n1,1\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "type": "int",
                    "filter": "it % 2 == 1"},
            }
        }, csv_in_string, csv_out_string)

    def test_map1(self):
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

    def test_map2(self):
        csv_in_string = "a,b\n1,2\n3,4"
        csv_out_string = "a,b\r\n0.5,2\r\n1.5,4\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "type": "float(it)",
                    "map": "it / 2"
                }
            }
        }, csv_in_string, csv_out_string)

    def test_rename(self):
        csv_in_string = "a\n1"
        csv_out_string = "A\r\n1\r\n"

        self._test_transformation({
            "cols": {
                "a": {
                    "rename": "A"
                },
            }
        }, csv_in_string, csv_out_string)

    def test_id(self):
        csv_in_string = "a\n1\n6\n9\n2\n7"
        csv_out_string = "a\r\n1\r\n2\r\n"

        self._test_transformation({
            "entity_filter": "COL_A < 5",
            "cols": {
                "a": {
                    "type": "int",
                    "id": "COL_A"
                }
            }
        }, csv_in_string, csv_out_string)

    #### COMPLEX

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

    def test_new_col(self):
        csv_in_string = "a\n1\n3\n-2"
        csv_out_string = "a,b\r\n1,2\r\n3,6\r\n-2,-4\r\n"

        self._test_transformation({
            "cols": {
                "a": {"type": "int"},
            },
            "new_cols": [
                {"id": "b", "formula_path": "a * 2"}
            ]
        }, csv_in_string, csv_out_string)

    def test_new_col_filter(self):
        csv_in_string = "a\n1\n3\n-2"
        csv_out_string = "a,c\r\n1,1\r\n-2,-1\r\n"

        self._test_regular_transformation({
            "cols": {
                "a": {"type": "int"},
            },
            "new_cols": [
                {"id": "b", "visible": False, "formula_path": "a * 2",
                 "filter": "it < 6"},
                {"id": "c", "formula_path": "if(a > 0, 1,if(a==0, 0, -1))"}
            ]
        }, csv_in_string, csv_out_string)

    def test_new_col_agg(self):
        csv_in_string = "a\n1\n-2\n3\n1\n-2\n3"
        csv_out_string1 = "a,b\r\n4,1\r\n2,-1\r\n"
        csv_out_string2 = "a,b\r\n2,-1\r\n4,1\r\n"

        ret = self._apply_regular_transformation({
            "cols": {
                "a": {"type": "int", "agg": "count"},
            },
            "new_cols": [
                {"id": "b", "formula_path": "if(a > 0, 1,if(a==0, 0, -1))"}
            ]
        }, csv_in_string)

        self.assertTrue(ret == csv_out_string1 or ret == csv_out_string2)

    def test_new_col_agg2(self):
        csv_in_string = "a\n1\n-2\n3\n1\n-2\n3"
        csv_out_string1 = "a,b\r\n4,1\r\n2,-1\r\n"
        csv_out_string2 = "a,b\r\n2,-1\r\n4,1\r\n"

        ret = self._apply_regular_transformation({
            "default_col": {"visible": False},
            "cols": {
                "a": {"visible": True, "type": "int", "agg": "count"},
            },
            "new_cols": [
                {"id": "b", "visible": True, "formula_path": "if(a > 0, 1,if(a==0, 0, -1))"}
            ]
        }, csv_in_string)

        self.assertTrue(ret == csv_out_string1 or ret == csv_out_string2)

    def test_new_col_entity_filter(self):
        csv_in_string = "a\n1\n3\n-2\n0"
        csv_out_string = "a,b\r\n3,9\r\n-2,4\r\n"

        self._test_regular_transformation({
            "entity_filter": "a != b",
            "cols": {
                "a": {"type": "int"},
            },
            "new_cols": [
                {"id": "b", "formula_path": "a * a"},
            ]
        }, csv_in_string, csv_out_string)

    def test_new_col_agg_filter(self):
        csv_in_string = "a\n1\n-2\n3\n1\n-2\n3"
        csv_out_string = "a,b\r\n4,1\r\n"

        self._test_regular_transformation({
            "agg_filter": "b == 1",
            "cols": {
                "a": {"type": "int", "agg": "count"},
            },
            "new_cols": [
                {"id": "b", "formula_path": "if(a > 0, 1,if(a==0, 0, -1))"}
            ]
        }, csv_in_string, csv_out_string)

    def test_new_col_agg_filter2(self):
        csv_in_string = "a\n1\n-2\n3\n1\n-2\n3"
        csv_out_string = "a,b\r\n4,1\r\n"

        self._test_regular_transformation({
            "agg_filter": "a == 4",
            "cols": {
                "a": {"type": "int", "agg": "count"},
            },
            "new_cols": [
                {"id": "b", "formula_path": "if(a > 0, 1,if(a==0, 0, -1))"}
            ]
        }, csv_in_string, csv_out_string)

class CSVTransformerAggTestCase(CSVTransformerTestCase):
    def test_sum(self):
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


class CSVTransformerParserTestCase(unittest.TestCase):
    def test_err_col_type(self):
        transformation_dict = {
            "cols": {
                "a": {
                    "type": "INT"
                }
            }
        }
        factory = TransformationBuilder(
            False, FUNC_BY_TYPE, FUNC_BY_AGG, BINOP_BY_NAME,
            PREFIX_UNOP_BY_NAME, INFIX_UNOP_BY_NAME
        )
        transformation = TransformationJsonParser(
            factory).parse(transformation_dict)
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
        factory = TransformationBuilder(
            False, FUNC_BY_TYPE, FUNC_BY_AGG, BINOP_BY_NAME,
            PREFIX_UNOP_BY_NAME, INFIX_UNOP_BY_NAME
        )
        transformation = TransformationJsonParser(
            factory).parse(transformation_dict)
        self.assertFalse(transformation.has_agg())


class CSVTransformerIntegrationTestCase(unittest.TestCase):
    def test_transform1(self):
        csv_in = {"encoding": "utf-8", "path": (
                FIXTURE_PATH / "StockEtablissementLiensSuccession_utf8.csv")}
        csv_out = {"path": FIXTURE_PATH / "test.csv"}
        transformation_dict = {
            "entity_filter": "date_lien_succ > date('2000-01-01')",
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
        f = ExpressionParser(BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
                             INFIX_UNOP_BY_NAME).parse("it * 2")
        self.assertEqual(6, f(3))


class HeaderTestCase(unittest.TestCase):
    def test_improve(self):
        self.assertEqual(["a_1", "a_2", "a_3"],
                         improve_header(["a", "a", "a"]))
        self.assertEqual(["a_2", "a_1", "a_3"],
                         improve_header(["a", "a_1", "a"]))
        self.assertEqual(["a_1", "_1", "b", "_2", "a_2", "_3"],
                         improve_header(["a", "", "b", "", "a", ""]))

    def test_add_fields(self):
        self.assertEqual(
            ['a', 'b', 'c', 'extra_1', 'extra_2', 'extra_3', 'extra_4',
             'extra_5'], add_fields(["a", "b", "c"], total=8))
        self.assertEqual(["a", "b", "c"],
                         add_fields(["a", "b", "c"], total=2))

    def test_normalize(self):
        self.assertEqual("un_test_effectue", normalize("Un Test  Effectué"))


if __name__ == '__main__':
    unittest.main()
