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
MainFilter = Callable[[TypedRow], bool]
ColType = Callable[[str], Any]
ColFilter = Callable[[Any], bool]
Expression = Callable[[Any], Any]
ColAgg = Callable[[List[Any]], Any]

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


class DefaultColumnTransformation:
    def __init__(self, visible: bool):
        self._visible = visible

    def is_visible(self) -> bool:
        return self._visible


class ColumnTransformation:
    def __init__(self, col_visible: bool, col_type: Optional[ColType],
                 col_filter: Optional[ColFilter],
                 col_map: Optional[Expression],
                 col_agg: Optional[ColAgg],
                 col_rename: Optional[str], col_id: Optional[str]):
        self._visible = col_visible
        self._type = col_type
        self._filter = col_filter
        self._map = col_map
        self._agg = col_agg
        self._rename = col_rename
        self._id = col_id

    def has_agg(self) -> bool:
        return self._agg is not None

    def rename(self, name: str) -> str:
        if self._rename is None:
            return name
        else:
            return self._rename

    def type_value(self, value_str: str) -> Any:
        if self._type is None:
            return value_str
        else:
            return self._type(value_str)

    def is_visible(self) -> bool:
        return self._visible

    def agg(self, values: List[Any]) -> Any:
        if self._agg is None:
            raise ValueError()
        else:
            return self._agg(values)

    def get_id(self) -> str:
        return self._id

    def col_filter(self, value: Any) -> bool:
        if self._filter is None:
            return True
        else:
            return self._filter(value)

    def col_map(self, value: Any) -> Any:
        if self._map is None:
            return value
        else:
            return self._map(value)


class Transformation:
    def __init__(self, main_filter: Optional[MainFilter],
                 default_column_transformation: Optional[
                     DefaultColumnTransformation],
                 col_transformation_by_name: Dict[str, ColumnTransformation], extra_prefix: str, extra_count: int):
        self._main_filter = main_filter
        self._default_column_transformation = default_column_transformation
        self._col_transformation_by_name = col_transformation_by_name
        self._extra_prefix = extra_prefix
        self._extra_count = extra_count
        self._d = {}

    def has_agg(self) -> bool:
        return any(self._col_is_agg(n)
                   for n in self._col_transformation_by_name)

    def add_fields(self, header: List[str]) -> List[str]:
        if self._extra_count > 0:
            return add_fields(header, self._extra_prefix, self._extra_count)
        else:
            return header

    def new_header(self, header: Iterable[str]) -> List[str]:
        return [self._col_rename(n) for n in header if self._col_is_visible(n)]

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
                if not self._col_is_agg(n) and self._col_is_visible(n)
            ])
            for n in self._col_transformation_by_name:
                if self._col_is_agg(n):
                    self._d.setdefault(key, {}).setdefault(n, []).append(row[n])

    def agg_rows(self) -> Iterator[TypedRow]:
        for key, values_by_name in self._d.items():
            row = dict(key)
            for name, values in values_by_name.items():
                row[name] = self._col_transformation_by_name[name].agg(values)

            yield self._rename(row)

    def _type_row(self, row: StrRow) -> StrRow:
        return {n: self._type_value(n, v) for n, v in row.items()}

    def _accept(self, row: TypedRow) -> bool:
        if self._main_filter is None:
            return all(
                self._col_accept(name, value) for name, value in row.items())
        else:
            value_by_id = {self._col_id(name): value for name, value in row.items()}
            if self._main_filter(value_by_id):
                return all(
                    self._col_accept(name, value) for name, value in row.items())
            else:
                return False

    def _type_value(self, name: str, value: str) -> Any:
        try:
            return self._col_transformation_by_name[name].type_value(value)
        except KeyError:
            return value

    def _map(self, row: TypedRow) -> TypedRow:
        return {
            n: self._col_map(n, v) for n, v in row.items()
        }

    def _rename(self, row: TypedRow) -> TypedRow:
        return {
            self._col_rename(n): v for n, v in row.items()
        }

    def _col_is_agg(self, name: str) -> bool:
        try:
            return self._col_transformation_by_name[name].has_agg()
        except KeyError:
            return False

    def _col_rename(self, name: str) -> str:
        try:
            return self._col_transformation_by_name[name].rename(name)
        except KeyError:
            return name

    def _col_is_visible(self, name: str) -> bool:
        try:
            return self._col_transformation_by_name[name].is_visible()
        except KeyError:
            if self._default_column_transformation is None:
                return True
            else:
                return self._default_column_transformation.is_visible()

    def _col_id(self, name: str) -> str:
        try:
            return self._col_transformation_by_name[name].get_id()
        except KeyError:
            return name

    def _col_accept(self, name: str, value: Any) -> bool:
        try:
            return self._col_transformation_by_name[name].col_filter(value)
        except KeyError:
            return True

    def _col_map(self, name: str, value: Any) -> Any:
        try:
            return self._col_transformation_by_name[name].col_map(value)
        except KeyError:
            return value


class MainFilterParser:
    _logger = logging.getLogger(__name__)

    def parse(self, main_filter_str: str) -> MainFilter:
        tokens = tokenize_expr(main_filter_str)
        tokens = shunting_yard(tokens)
        return lambda r: evaluate(tokens, r)


class RiskyMainFilterParser:
    _logger = logging.getLogger(__name__)

    def parse(self, main_filter_str: str) -> MainFilter:
        return lambda r: eval(main_filter_str, {}, r)


class ExpressionParser:
    _logger = logging.getLogger(__name__)

    def parse(self, expression_str: str) -> Expression:
        tokens = tokenize_expr(expression_str)
        tokens = shunting_yard(tokens)
        return lambda v: evaluate(tokens, {"it": v})


class RiskyExpressionParser:
    _logger = logging.getLogger(__name__)

    def parse(self, expression_str: str) -> Expression:
        return lambda v: eval(expression_str, {}, {"it": v})


class DefaultColumnTransformationParser:
    def __init__(self):
        self._visible = None

    def parse(self, default_col: JSONValue):
        self._visible = default_col.get("visible", True)

        return DefaultColumnTransformation(self._visible)


class ColumnTransformationParser:
    _logger = logging.getLogger(__name__)

    def __init__(self, risky: bool):
        self._risky = risky
        self._visible = True
        self._type = cast(Optional[ColType], None)
        self._filter = cast(Optional[ColFilter], None)
        self._map = cast(Optional[Expression], None)
        self._agg = cast(Optional[ColAgg], None)
        self._rename = cast(Optional[str], None)
        self._id = cast(Optional[str], None)

    def parse(self, json_col: JSONValue):
        self._visible = json_col.get("visible", True)

        try:
            col_type_str = json_col["type"]
        except KeyError:
            pass
        else:
            self._parse_col_type(col_type_str)

        try:
            col_filter_str = json_col["filter"]
        except KeyError:
            pass
        else:
            self._parse_col_filter(col_filter_str)

        try:
            col_map_str = json_col["map"]
        except KeyError:
            pass
        else:
            self._parse_col_map(col_map_str)

        try:
            col_agg_str = json_col["agg"]
        except KeyError:
            pass
        else:
            self._parse_col_agg(col_agg_str)

        try:
            col_rename_str = json_col["rename"]
        except KeyError:
            pass
        else:
            self._parse_col_rename(col_rename_str)

        try:
            col_id_str = json_col["id"]
        except KeyError:
            pass
        else:
            self._parse_col_id(col_id_str)

        return ColumnTransformation(
            self._visible, self._type, self._filter, self._map, self._agg,
            self._rename, self._id,
        )

    def _parse_col_type(self, col_type_str: str):
        try:
            self._type = FUNC_BY_TYPE[col_type_str]
        except KeyError:
            parser = self._get_parser()
            self._type = parser.parse(col_type_str)

    def _parse_col_filter(self, col_filter_str: str):
        parser = self._get_parser()
        self._filter = parser.parse(col_filter_str)

    def _get_parser(self):
        if self._risky:
            parser = RiskyExpressionParser()
        else:
            parser = ExpressionParser()
        return parser

    def _parse_col_map(self, col_map_str: str):
        parser = self._get_parser()
        self._map = parser.parse(col_map_str)

    def _parse_col_agg(self, col_agg_str: str):
        try:
            self._agg = FUNC_BY_AGG[col_agg_str]
        except KeyError as e:
            self._logger.exception("Agg error")

    def _parse_col_rename(self, col_rename: str):
        self._rename = col_rename

    def _parse_col_id(self, col_id: str):
        self._id = col_id


class TransformationParser:
    _logger = logging.getLogger(__name__)

    def __init__(self, risky: bool):
        self._risky = risky
        self._main_filter = cast(Optional[MainFilter], None)
        self._default_column_transformation = cast(
            Optional[DefaultColumnTransformation], None)
        self._col_transformation_by_name = cast(Dict[str, ColumnTransformation],
                                                {})
        self._extra_prefix = "extra"
        self._extra_count = 1024

    def parse(self, json_transformation: JSONValue) -> Transformation:
        try:
            self._parse_main_filter(json_transformation["filter"])
        except KeyError:
            pass
        default_col = json_transformation.get("default_col", {})
        self._parse_default_col(default_col)

        cols = json_transformation.get("cols", {})
        for name, col in cols.items():
            self._col_transformation_by_name[
                name] = ColumnTransformationParser(self._risky).parse(col)

        extra = json_transformation.get("extra", {})
        self._parse_extra(extra)

        return Transformation(self._main_filter,
                              self._default_column_transformation,
                              self._col_transformation_by_name, self._extra_prefix, self._extra_count)

    def _parse_main_filter(self, main_filter_str: str):
        if self._risky:
            self._main_filter = RiskyMainFilterParser().parse(main_filter_str)
        else:
            self._main_filter = MainFilterParser().parse(main_filter_str)

    def _parse_default_col(self, defaut_col: JSONValue):
        self._default_column_transformation = DefaultColumnTransformationParser().parse(
            defaut_col)

    def _parse_extra(self, extra: JSONValue):
        self._extra_count = extra.get("count", 1024)
        self._extra_prefix = extra.get("prefix", "extra")


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
        new_header = transformation.add_fields(header)
        new_header = improve_header(new_header)
        new_header = transformation.new_header(new_header)
        writer.writerow(new_header)
        if transformation.has_agg():
            for row in itertools.islice(reader, limit):
                row = dict(zip(new_header, row))
                transformation.take(row)

            for row in transformation.agg_rows():
                writer.writerow([row.get(n, "") for n in new_header])
        else:
            for row in itertools.islice(reader, limit):
                row = dict(zip(header, row))
                row = transformation.transform(row)
                if row is not None:
                    writer.writerow([row.get(n, "") for n in new_header])


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
    s = SPACE_REGEX.sub("_", s)
    s = s.lower()
    return s


def add_fields(fields, prefix="extra", total=1024):
    if len(fields) >= total:
        return fields

    return fields + ["{}_{}".format(prefix, i) for i in
                     range(1, total - len(fields) + 1)]
