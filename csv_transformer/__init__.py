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
import csv
import itertools

from csv_transformer.en_functions import (
    FUNC_BY_TYPE, FUNC_BY_AGG, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
    INFIX_UNOP_BY_NAME)
from csv_transformer.transformation import (
    TransformationJsonParser, improve_header, JSONValue, TransformationBuilder)


def main(csv_in: JSONValue, transformation_dict: JSONValue, csv_out: JSONValue,
         risky=False, limit=None):
    transformation_builder = TransformationBuilder(
        risky, FUNC_BY_TYPE, FUNC_BY_AGG, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
        INFIX_UNOP_BY_NAME)
    trans = TransformationJsonParser(transformation_builder).parse(transformation_dict)

    in_encoding = csv_in.pop("encoding", "utf-8")
    in_path = csv_in.pop("path")
    out_encoding = csv_out.pop("encoding", "utf-8")
    out_path = csv_out.pop("path")
    with in_path.open("r", encoding=in_encoding) as s, \
            out_path.open("w", encoding=out_encoding, newline="") as d:
        writer = csv.writer(d, **csv_out)
        reader = csv.reader(s, **csv_in)
        header = next(reader)
        clean_header = trans.add_fields(header)
        clean_header = improve_header(clean_header)
        clean_header = trans.extend_header(clean_header)

        # write file header
        col_renames = trans.file_header(clean_header)
        writer.writerow(col_renames)

        # the id header
        col_ids = trans.col_ids(clean_header)
        visible_col_ids = trans.visible_col_ids(clean_header)

        if trans.has_agg():
            for row in itertools.islice(reader, limit):
                value_by_id = dict(zip(col_ids, row))
                trans.take_or_ignore(value_by_id)

            for value_by_id in trans.agg_rows():
                if trans.agg_filter(value_by_id):
                    writer.writerow(
                        [value_by_id.get(i, "") for i in visible_col_ids])
        else:
            for row in itertools.islice(reader, limit):
                value_by_id = dict(zip(col_ids, row))
                value_by_id = trans.transform(value_by_id)
                if value_by_id is not None:
                    writer.writerow(
                        [value_by_id.get(i, "") for i in visible_col_ids])
