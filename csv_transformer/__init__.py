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
from abc import ABC, abstractmethod

from csv_transformer.en_functions import (
    FUNC_BY_TYPE, FUNC_BY_AGG, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
    INFIX_UNOP_BY_NAME)
from csv_transformer.transformation import (
    TransformationJsonParser, improve_header, JSONValue, TransformationBuilder,
    Transformation, Expression)


def main(csv_in: JSONValue, transformation_dict: JSONValue, csv_out: JSONValue,
         risky: bool = False, limit: int = None):
    executor = create_executor(transformation_dict, csv_out, risky, limit)
    executor.execute(csv_in)


def create_executor(transformation_dict: JSONValue, csv_out: JSONValue,
                    risky: bool = False, limit: int = None
                    ) -> "Executor":
    transformation_builder = TransformationBuilder(
        risky, FUNC_BY_TYPE, FUNC_BY_AGG, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
        INFIX_UNOP_BY_NAME)
    transformation = TransformationJsonParser(transformation_builder).parse(
        transformation_dict)

    if "path" in csv_out:
        return ExecutorOnce(transformation, csv_out, limit)
    else:
        expression_parser = transformation_builder.expression_parser()
        formula_path = expression_parser.parse(csv_out.pop("formula_path", "t"))
        return ExecutorMany(transformation, csv_out, formula_path, limit)


class Executor(ABC):
    @abstractmethod
    def execute(self, csv_in: JSONValue):
        pass


class ExecutorOnce(Executor):
    def __init__(self, transformation: Transformation, csv_out: JSONValue,
                 limit: int = None):
        self._transformation = transformation
        self._csv_out = csv_out
        self._limit = limit

    def execute(self, csv_in: JSONValue):
        in_encoding = csv_in.pop("encoding", "utf-8")
        in_path = csv_in.pop("path")
        out_encoding = self._csv_out.pop("encoding", in_encoding)
        out_path = self._csv_out.pop("path")

        with in_path.open("r", encoding=in_encoding) as s, \
                out_path.open("w", encoding=out_encoding, newline="") as d:
            writer = csv.writer(d, **self._csv_out)
            reader = csv.reader(s, **csv_in)
            execute_rw(reader, self._transformation, writer, self._limit)


class ExecutorMany(Executor):
    def __init__(self, transformation: Transformation, csv_out: JSONValue,
                 formula_path: Expression,
                 limit: int = None):
        self._transformation = transformation
        self._csv_out = csv_out
        self._formula_path = formula_path
        self._limit = limit

    def execute(self, csv_in: JSONValue):
        in_encoding = csv_in.pop("encoding", "utf-8")
        in_path = csv_in.pop("path")
        out_encoding = self._csv_out.pop("encoding", in_encoding)
        out_path = self._formula_path(in_path)

        with in_path.open("r", encoding=in_encoding) as s, \
                out_path.open("w", encoding=out_encoding, newline="") as d:
            writer = csv.writer(d, **self._csv_out)
            reader = csv.reader(s, **csv_in)
            execute_rw(reader, self._transformation, writer, self._limit)


def execute_rw(reader: csv.reader, transformation: Transformation,
               writer: csv.writer, limit: int = None):
    header = next(reader)
    clean_header = transformation.add_fields(header)
    clean_header = improve_header(clean_header)
    # write file header
    col_renames = transformation.file_header(clean_header)
    writer.writerow(col_renames)
    # the id header
    col_ids = transformation.col_ids(clean_header)
    visible_col_ids = transformation.visible_col_ids(clean_header)
    if transformation.has_agg():
        for row in itertools.islice(reader, limit):
            value_by_id = dict(zip(col_ids, row))
            transformation.take_or_ignore(value_by_id)

        for value_by_id in transformation.agg_rows():
            if transformation.agg_filter(value_by_id):
                writer.writerow(
                    [value_by_id.get(i, "") for i in visible_col_ids])
    else:
        for row in itertools.islice(reader, limit):
            value_by_id = dict(zip(col_ids, row))
            value_by_id = transformation.transform(value_by_id)
            if value_by_id is not None:
                writer.writerow(
                    [value_by_id.get(i, "") for i in visible_col_ids])
