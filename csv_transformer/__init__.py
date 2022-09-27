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
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, TextIO, Iterable, Iterator

from csv_transformer.en_functions import (
    FUNC_BY_TYPE, FUNC_BY_AGG, BINOP_BY_NAME, PREFIX_UNOP_BY_NAME,
    INFIX_UNOP_BY_NAME)
from csv_transformer.transformation import (
    TransformationJsonParser, improve_header, JSONValue, TransformationBuilder,
    Transformation, Expression, ExpressionParser, TypedRow)


class CsvIn:
    def __init__(self, path: Path, encoding: str, fmtparams: JSONValue):
        self.path = path
        self.encoding = encoding
        self.fmtparams = fmtparams

    @contextmanager
    def reader(self) -> csv.reader:
        with self.path.open("r", encoding=self.encoding) as s:
            yield csv.reader(s, **self.fmtparams)


class CsvOut:
    def __init__(self, path_func: Callable[[Path], Path],
                 encoding_func: Callable[[str], str], fmtparams: JSONValue):
        self._path_func = path_func
        self._encoding_func = encoding_func
        self.fmtparams = fmtparams

    def path(self, in_path: Path) -> Path:
        return self._path_func(in_path)

    def encoding(self, in_encoding: str) -> str:
        return self._encoding_func(in_encoding)

    @contextmanager
    def writer(self, csv_in: CsvIn) -> csv.writer:
        in_encoding = csv_in.encoding
        in_path = csv_in.path
        out_encoding = self.encoding(in_encoding)
        out_path = self.path(in_path)
        with out_path.open("w", encoding=out_encoding, newline="") as d:
            yield csv.writer(d, **self.fmtparams)


def parse_json_csv_in(csv_in: JSONValue) -> CsvIn:
    return CsvIn(csv_in.pop("path"), csv_in.pop("encoding", "utf-8"),
                 csv_in)


def parse_json_csv_out(expression_parser: ExpressionParser,
                       csv_out: JSONValue) -> CsvOut:
    if "path" in csv_out:
        path = csv_out.pop("path")

        def path_func(_in_path: Path) -> Path:
            return path
    else:
        formula_path_str = csv_out.pop("formula_path")
        formula_path = expression_parser.parse(formula_path_str)

        def path_func(in_path: Path) -> Path:
            return formula_path(in_path)

    if "encoding" in csv_out:
        def encoding_func(_in_encoding):
            return csv_out.pop("encoding")
    else:
        def encoding_func(in_encoding):
            return in_encoding

    return CsvOut(path_func, encoding_func, csv_out)


def main(csv_in_dict: JSONValue, transformation_dict: JSONValue,
         csv_out_dict: JSONValue,
         risky: bool = False, limit: int = None):
    executor = create_executor(transformation_dict, csv_out_dict, risky, limit)

    csv_in = parse_json_csv_in(csv_in_dict)
    executor.execute(csv_in)


def create_executor(transformation_dict: JSONValue, csv_out_dict: JSONValue,
                    risky: bool = False, limit: int = None):
    transformation_builder = TransformationBuilder(
        risky, "it", FUNC_BY_TYPE, FUNC_BY_AGG, BINOP_BY_NAME,
        PREFIX_UNOP_BY_NAME, INFIX_UNOP_BY_NAME)
    transformation = TransformationJsonParser(transformation_builder).parse(
        transformation_dict)
    csv_out = parse_json_csv_out(transformation_builder.expression_parser(),
                                 csv_out_dict)
    executor = Executor(transformation, csv_out, limit)
    return executor


class Executor:
    def __init__(self, transformation: Transformation, csv_out: CsvOut,
                 limit: int = None):
        self._transformation = transformation
        self._csv_out = csv_out
        self._limit = limit

    def execute(self, csv_in: CsvIn) -> Path:
        with csv_in.reader() as reader, self._csv_out.writer(csv_in) as writer:
            # TODO: yield rows
            self.execute_rw(reader, self._transformation, writer, self._limit)

        return self._csv_out.path(csv_in.path)

    def execute_rw(self, reader: csv.reader, transformation: Transformation,
                   writer: csv.writer, limit: int = None):
        header = next(reader)
        clean_header = transformation.add_fields(header)
        clean_header = improve_header(clean_header)
        # write file header
        file_header = transformation.file_header(clean_header)
        writer.writerow(file_header)
        # the id header
        col_ids = transformation.col_ids(clean_header)
        visible_col_ids = transformation.visible_col_ids(clean_header)
        if transformation.has_agg():
            if transformation.has_order():
                for value_by_id in sorted(
                        self._row_generator(reader, limit, transformation,
                                            col_ids),
                        key=transformation.create_key(col_ids)):
                    writer.writerow(
                        [value_by_id.get(i, "") for i in visible_col_ids])
            else:
                for value_by_id in self._row_generator(reader, limit,
                                                       transformation,
                                                       col_ids):
                    writer.writerow(
                        [value_by_id.get(i, "") for i in visible_col_ids])
        else:
            if transformation.has_order():
                for value_by_id in sorted(
                        self._agg_generator(reader, limit, transformation,
                                            col_ids),
                        key=transformation.create_key(col_ids)):
                    writer.writerow(
                        [value_by_id.get(i, "") for i in visible_col_ids])
            else:
                for value_by_id in self._agg_generator(reader, limit,
                                                       transformation,
                                                       col_ids):
                    writer.writerow(
                        [value_by_id.get(i, "") for i in visible_col_ids])

    def _row_generator(self, reader: TextIO, limit: int,
                       transformation: Transformation,
                       col_ids: Iterable[str]
                       ) -> Iterator[TypedRow]:
        for row in itertools.islice(reader, limit):
            value_by_id = dict(zip(col_ids, row))
            transformation.take_or_ignore(value_by_id)

        for value_by_id in transformation.agg_rows():
            if transformation.agg_filter(value_by_id):
                yield value_by_id

    def _agg_generator(self, reader: TextIO, limit: int,
                       transformation: Transformation,
                       col_ids: Iterable[str]
                       ) -> Iterator[TypedRow]:
        for row in itertools.islice(reader, limit):
            value_by_id = dict(zip(col_ids, row))
            value_by_id = transformation.transform(value_by_id)
            if value_by_id is not None:
                yield value_by_id
