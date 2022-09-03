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
import collections
import csv
import datetime as dt
import itertools
import logging
import re
import statistics
from typing import (Union, Mapping, List, Callable, Any, cast, Dict, Iterable,
                    Optional, Iterator)

from csv_transformer.simple_eval import tokenize_expr, shunting_yard, evaluate

StrRow = Mapping[str, str]
TypedRow = Mapping[str, Any]

# https://www.postgresql.org/docs/current/functions-aggregate.html
FUNC_BY_AGG = {
    "all": all,
    "any": any,
    "first": lambda xs: xs[0],
    "last": lambda xs: xs[-1],
    "count": len,
    "count_distinct": len,
    "sum": sum,
    "mean": statistics.mean,
    "median": statistics.median,
    "stdev": statistics.stdev,
    "pstdev": statistics.pstdev,
    "variance": statistics.variance,
    "min": min,
    "max": max,
    "string_agg": lambda xs: ", ".join(map(str, xs))
}

FUNC_BY_TYPE = {
    "int": int,
    "float_iso": float,
    "float": float,
    "date": dt.date.fromisoformat,
    "date_iso": dt.date.fromisoformat
}

JSONValue = Union[
    int, float, str, bool, Dict[str, "JSONValue"], List["JSONValue"]]


class Transformation:
    def __init__(self, main_filter, invisible_names, col_type_by_name,
                 col_filter_by_name, col_map_by_name, col_agg_by_name,
                 col_rename_by_name: Mapping[str, str],
                 col_id_by_name: Mapping[str, str]):
        self._main_filter = main_filter
        self._invisible_names = invisible_names
        self._col_type_by_name = col_type_by_name
        self._col_filter_by_name = col_filter_by_name
        self._col_map_by_name = col_map_by_name
        self._col_agg_by_name = col_agg_by_name
        self._col_rename_by_name = col_rename_by_name
        self._col_id_by_name = col_id_by_name
        self._d = {}

    def has_agg(self) -> bool:
        return bool(self._col_agg_by_name)

    def new_header(self, header: Iterable[str]) -> List[str]:
        return [self._col_rename_by_name.get(n, n) for n in header if
                n not in self._invisible_names]

    def transform(self, row: StrRow) -> Optional[TypedRow]:
        row = self._type_row(row)
        if self._accept(row):
            return self._rename(self._map(row))
        else:
            return None

    def take(self, row: StrRow):
        row = self._type_row(row)
        if self._accept(row):
            key = tuple([
                (n, row[n]) for n in row
                if n not in self._col_agg_by_name
                   and n not in self._invisible_names
            ])
            for n in self._col_agg_by_name:
                self._d.setdefault(key, {}).setdefault(n, []).append(row[n])

    def agg_rows(self) -> Iterator[TypedRow]:
        for key, values_by_name in self._d.items():
            row = dict(key)
            for name, values in values_by_name.items():
                row[name] = self._col_agg_by_name[name](values)

            yield self._rename(row)

    def _type_row(self, row: StrRow) -> StrRow:
        return {n: self._type_value(n, v) for n, v in row.items()}

    def _type_value(self, name: str, value: str) -> Any:
        try:
            return self._col_type_by_name[name](value)
        except KeyError:
            return value

    def _accept(self, row: TypedRow) -> bool:
        if not (self._main_filter is None
                or self._main_filter(
                    {
                        self._col_id_by_name.get(name, name): value
                        for name, value in row.items()
                    })
        ):
            return False

        for name, value in row.items():
            try:
                if not self._col_filter_by_name[name](value):
                    return False
            except KeyError:
                pass
        return True

    def _map(self, row: TypedRow) -> TypedRow:
        return {
            n: self._col_map_by_name.get(n, lambda x: x)(v) for n, v in row.items()
        }

    def _rename(self, row: TypedRow) -> TypedRow:
        return {
            self._col_rename_by_name.get(n, n): v for n, v in row.items()
        }


class MainFilterParser:
    _logger = logging.getLogger(__name__)

    def parse(self, main_filter_str: str) -> Callable[[TypedRow], bool]:
        tokens = tokenize_expr(main_filter_str)
        tokens = shunting_yard(tokens)
        return lambda r: evaluate(tokens, r)


class RiskyMainFilterParser:
    _logger = logging.getLogger(__name__)

    def parse(self, main_filter_str: str) -> Callable[[TypedRow], bool]:
        return lambda r: eval(main_filter_str, {}, r)


class ExpressionParser:
    _logger = logging.getLogger(__name__)

    def parse(self, col_filter_str: str) -> Callable[[Any], Any]:
        tokens = tokenize_expr(col_filter_str)
        tokens = shunting_yard(tokens)
        return lambda v: evaluate(tokens, {"it": v})


class RiskyExpressionParser:
    _logger = logging.getLogger(__name__)

    def parse(self, col_filter_str: str) -> Callable[[Any], Any]:
        return lambda v: eval(col_filter_str, {}, {"it": v})


class TransformationParser:
    _logger = logging.getLogger(__name__)

    def __init__(self, risky: bool):
        self._risky = risky
        self._main_filter = None
        self._col_type_by_name = cast(Dict[str, Callable[[str], Any]], {})
        self._col_filter_by_name = cast(Dict[str, Callable[[Any], bool]], {})
        self._col_map_by_name = cast(Dict[str, Callable[[Any], Any]], {})
        self._col_agg_by_name = cast(Dict[str, Callable[[List[Any]], Any]], {})
        self._col_rename_by_name = cast(Dict[str, str], {})
        self._col_id_by_name = cast(Dict[str, str], {})
        self._invisible_names = cast(List[str], [])

    def parse(self, json_transformation: JSONValue):
        try:
            self._parse_main_filter(json_transformation["filter"])
        except KeyError:
            pass
        cols = json_transformation.get("cols", {})
        for name, col in cols.items():
            self._parse_col(name, col)

        return Transformation(self._main_filter, self._invisible_names,
                              self._col_type_by_name, self._col_filter_by_name,
                              self._col_map_by_name, self._col_agg_by_name,
                              self._col_rename_by_name, self._col_id_by_name)

    def _parse_main_filter(self, main_filter_str: str):
        if self._risky:
            self._main_filter = RiskyMainFilterParser().parse(main_filter_str)
        else:
            self._main_filter = MainFilterParser().parse(main_filter_str)

    def _parse_col(self, name: str, json_col: JSONValue):
        visible = json_col.get("visible", True)
        if not visible:
            self._invisible_names.append(name)

        try:
            col_type_str = json_col["type"]
        except KeyError:
            pass
        else:
            self._parse_col_type(name, col_type_str)

        try:
            col_filter_str = json_col["filter"]
        except KeyError:
            pass
        else:
            self._parse_col_filter(name, col_filter_str)

        try:
            col_map_str = json_col["map"]
        except KeyError:
            pass
        else:
            self._parse_col_map(name, col_map_str)

        try:
            col_agg_str = json_col["agg"]
        except KeyError:
            pass
        else:
            self._parse_col_agg(name, col_agg_str)

        try:
            col_rename_str = json_col["rename"]
        except KeyError:
            pass
        else:
            self._parse_col_rename(name, col_rename_str)

        try:
            col_id_str = json_col["id"]
        except KeyError:
            pass
        else:
            self._parse_col_id(name, col_id_str)

    def _parse_col_type(self, name: str, col_type_str: str):
        try:
            self._col_type_by_name[name] = FUNC_BY_TYPE[col_type_str]
        except KeyError:
            parser = self._get_parser()
            self._col_type_by_name[name] = parser.parse(col_type_str)

    def _parse_col_filter(self, name: str, col_filter_str: str):
        parser = self._get_parser()
        self._col_filter_by_name[name] = parser.parse(col_filter_str)

    def _get_parser(self):
        if self._risky:
            parser = RiskyExpressionParser()
        else:
            parser = ExpressionParser()
        return parser

    def _parse_col_map(self, name: str, col_map_str: str):
        parser = self._get_parser()
        self._col_map_by_name[name] = parser.parse(col_map_str)

    def _parse_col_agg(self, name: str, col_agg_str: str):
        try:
            self._col_agg_by_name[name] = FUNC_BY_AGG[col_agg_str]
        except KeyError as e:
            TransformationParser._logger.exception("Agg error")

    def _parse_col_rename(self, name: str, col_rename: str):
        self._col_rename_by_name[name] = col_rename

    def _parse_col_id(self, name: str, col_id: str):
        self._col_id_by_name[name] = col_id


def main(csv_in: JSONValue, transformation_dict: JSONValue, csv_out: JSONValue,
         risky=False, limit=None):
    transformation = TransformationParser(risky).parse(transformation_dict)

    in_encoding = csv_in.pop("encoding", "utf-8")
    in_path = csv_in.pop("path")
    out_encoding = csv_out.pop("encoding", "utf-8")
    out_path = csv_out.pop("path")
    with in_path.open("r", encoding=in_encoding) as s, \
            out_path.open("w", encoding=out_encoding, newline="") as d:
        writer = csv.writer(d, **csv_out)
        reader = csv.reader(s, **csv_in)
        header = next(reader)
        header = improve_header(header)
        new_header = transformation.new_header(header)
        writer.writerow(new_header)
        if transformation.has_agg():
            for row in itertools.islice(reader, limit):
                row = dict(zip(header, row))
                transformation.take(row)

            for row in transformation.agg_rows():
                writer.writerow([row[n] for n in new_header])
        else:
            for row in itertools.islice(reader, limit):
                row = dict(zip(header, row))
                row = transformation.transform(row)
                if row is not None:
                    writer.writerow([row[n] for n in new_header])


SUFFIX_REGEX = re.compile(r"^(.*)_(\d+)$")


def improve_header(header: List[str]) -> List[str]:
    c = collections.Counter(header)
    duplicates = set(k for k, v in c.items() if v > 1)
    seen = set(header)
    new_header = []
    for f in header:
        if f in duplicates:
            while f in seen:
                m = SUFFIX_REGEX.match(f)
                if m:
                    base = m.group(1)
                    v = int(m.group(2))
                else:
                    base = f
                    v = 0
                f = "{}_{}".format(base, v + 1)

        new_header.append(f)
        seen.add(f)
    return new_header


SPACE_REGEX = re.compile(r"\s+")


def normalize(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode(
        'ascii')
    s= SPACE_REGEX.sub("_", s)
    s = s.lower()
    return s
