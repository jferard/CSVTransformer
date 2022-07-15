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
from pathlib import Path

from csv_transformer import main


class CSVTransformerTestCase(unittest.TestCase):
    def test_transform(self):
        csv_in = {"encoding": "utf-8", "path": Path(
            "fixture/StockEtablissementLiensSuccession_utf8.csv")}
        csv_out = {"path": Path("fixture/test.csv")}
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


if __name__ == '__main__':
    unittest.main()
