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
import logging
import re
from typing import (Mapping, List, Callable, Any, cast, Dict, Iterable,
                    Optional, Iterator, Union)

from csv_transformer.functions import id_func, true_func, normalize
from csv_transformer.simple_eval import (
    tokenize_expr, evaluate, ShuntingYard)

JSONValue = Union[
    int, float, str, bool, Dict[str, "JSONValue"], List["JSONValue"]]

StrRow = Mapping[str, str]
TypedRow = Mapping[str, Any]
MainFilter = Callable[[TypedRow], bool]
ColType = Callable[[str], Any]
ColFilter = Callable[[Any], bool]
ColRename = Callable[[str], str]
Expression = Callable[[Any], Any]
ColAgg = Callable[[List[Any]], Any]


# https://www.postgresql.org/docs/current/functions-aggregate.html


class DefaultColumnTransformation:
    def __init__(self, visible: bool, normalize: bool):  # type
        self._visible = visible
        self._normalize = normalize

    def is_visible(self) -> bool:
        return self._visible

    def rename(self, name: str) -> str:
        if self._normalize:
            return normalize(name)
        else:
            return id_func(name)


class ColumnTransformation:
    def __init__(self, col_visible: bool, col_type: ColType,
                 col_filter: ColFilter,
                 col_map: Expression,
                 col_agg: Optional[ColAgg],
                 col_rename: ColRename, col_id: ColRename):
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
        return self._rename(name)

    def type_value(self, value_str: str) -> Any:
        return self._type(value_str)

    def is_visible(self) -> bool:
        return self._visible

    def agg(self, values: List[Any]) -> Any:
        return self._agg(values)

    def get_id(self, name: str) -> str:
        return self._id(name)

    def col_filter(self, value: Any) -> bool:
        return self._filter(value)

    def col_map(self, value: Any) -> Any:
        return self._map(value)


class Transformation:
    def __init__(self, main_filter: MainFilter,
                 default_column_transformation: DefaultColumnTransformation,
                 col_transformation_by_name: Dict[str, ColumnTransformation],
                 extra_prefix: str, extra_count: int):
        self._main_filter = main_filter
        self._default_column_transformation = default_column_transformation
        self._col_transformation_by_name = col_transformation_by_name
        self._col_transformation_by_id = {self._col_id(n): v for n, v in
                                          col_transformation_by_name.items()}
        self._extra_prefix = extra_prefix
        self._extra_count = extra_count
        self._d = {}

    def has_agg(self) -> bool:
        return any(self._col_is_agg(col_id)
                   for col_id in self._col_transformation_by_id)

    def add_fields(self, header: List[str]) -> List[str]:
        if self._extra_count > 0:
            return add_fields(header, self._extra_prefix, self._extra_count)
        else:
            return header

    def file_header(self, header: Iterable[str]) -> List[Optional[str]]:
        return [self._col_rename(n) for n in header
                if self._col_is_visible_by_name(n)]

    def col_ids(self, header: Iterable[str]) -> List[Optional[str]]:
        return [self._col_id(n) for n in header]

    def visible_col_ids(self, header: Iterable[str]) -> List[Optional[str]]:
        return [self._col_id(n) for n in header
                if self._col_is_visible_by_name(n)]

    def transform(self, value_by_id: StrRow) -> Optional[TypedRow]:
        value_by_id = self._type_row(value_by_id)
        typed_value_by_id = self._map(value_by_id)
        if self._filter(typed_value_by_id):
            return typed_value_by_id
        else:
            return None

    def _type_row(self, value_by_id: StrRow) -> TypedRow:
        return {i: self._type_value(i, v) for i, v in value_by_id.items()}

    def _type_value(self, col_id: str, value: str) -> Any:
        try:
            return self._col_transformation_by_id[col_id].type_value(value)
        except KeyError:
            return value

    def _filter(self, typed_value_by_id: TypedRow) -> bool:
        if self._main_filter(typed_value_by_id):
            return all(
                self._col_filter(col_id, value) for col_id, value in
                typed_value_by_id.items())
        else:
            return False

    def _rename(self, row: TypedRow) -> TypedRow:
        """Deprecated"""
        return {
            self._col_rename(n): v for n, v in row.items()
        }

    def _map(self, typed_value_by_id: TypedRow) -> TypedRow:
        return {
            n: self._col_map(n, v) for n, v in typed_value_by_id.items()
        }

    def take_or_ignore(self, value_by_id: StrRow):
        value_by_id = self._type_row(value_by_id)
        typed_value_by_id = self._map(value_by_id)
        if self._filter(typed_value_by_id):
            key = tuple([
                (i, typed_value_by_id[i]) for i in typed_value_by_id
                if not self._col_is_agg(i) and self._col_is_visible_by_id(i)
            ])
            for col_id in self._col_transformation_by_id:
                if self._col_is_agg(col_id):
                    self._d.setdefault(
                        key, {}).setdefault(
                        col_id, []).append(typed_value_by_id[col_id])

    def agg_rows(self) -> Iterator[TypedRow]:
        for key, values_by_id in self._d.items():
            row = dict(key)
            for col_id, values in values_by_id.items():
                row[col_id] = self._col_transformation_by_id[
                    col_id].agg(values)

            yield self._rename(row)

    def _col_is_agg(self, col_id: str) -> bool:
        try:
            return self._col_transformation_by_id[col_id].has_agg()
        except KeyError:
            return False

    def _col_rename(self, name: str) -> str:
        try:
            return self._col_transformation_by_name[name].rename(name)
        except KeyError:
            return self._default_column_transformation.rename(name)

    def _col_is_visible_by_name(self, name: str) -> bool:
        try:
            return self._col_transformation_by_name[name].is_visible()
        except KeyError:
            return self._default_column_transformation.is_visible()

    def _col_id(self, name: str) -> str:
        try:
            return self._col_transformation_by_name[name].get_id(name)
        except KeyError:
            return self._default_column_transformation.rename(name)

    def _col_is_visible_by_id(self, col_id: str) -> bool:
        try:
            return self._col_transformation_by_id[col_id].is_visible()
        except KeyError:
            return self._default_column_transformation.is_visible()

    def _col_filter(self, col_id: str, value: Any) -> bool:
        try:
            return self._col_transformation_by_id[col_id].col_filter(value)
        except KeyError:
            return True

    def _col_map(self, col_id: str, value: Any) -> Any:
        try:
            return self._col_transformation_by_id[col_id].col_map(value)
        except KeyError:
            return value


class MainFilterParser:
    _logger = logging.getLogger(__name__)

    def __init__(self, binop_by_name, prefix_unop_by_name,
                 infix_unop_by_name):
        self.binop_by_name = binop_by_name
        self.prefix_unop_by_name = prefix_unop_by_name
        self.infix_unop_by_name = infix_unop_by_name

    def parse(self, main_filter_str: str) -> MainFilter:
        tokens = tokenize_expr(main_filter_str)
        tokens = ShuntingYard(False, self.binop_by_name,
                              self.prefix_unop_by_name,
                              self.infix_unop_by_name).process(tokens)
        return lambda r: evaluate(tokens, r)


class RiskyMainFilterParser:
    _logger = logging.getLogger(__name__)

    def parse(self, main_filter_str: str) -> MainFilter:
        return lambda r: eval(main_filter_str, {}, r)


class ExpressionParser:
    _logger = logging.getLogger(__name__)

    def __init__(self, binop_by_name, prefix_unop_by_name,
                 infix_unop_by_name):
        self.binop_by_name = binop_by_name
        self.prefix_unop_by_name = prefix_unop_by_name
        self.infix_unop_by_name = infix_unop_by_name

    def parse(self, expression_str: str) -> Expression:
        tokens = tokenize_expr(expression_str)
        tokens = ShuntingYard(False, self.binop_by_name,
                              self.prefix_unop_by_name,
                              self.infix_unop_by_name).process(tokens)
        return lambda v: evaluate(tokens, {"it": v})


class RiskyExpressionParser:
    _logger = logging.getLogger(__name__)

    def parse(self, expression_str: str) -> Expression:
        return lambda v: eval(expression_str, {}, {"it": v})


class DefaultColumnTransformationParser:
    def __init__(self):
        self._visible = None
        self._normalize = None
        self._type = None

    def parse(self, default_col: JSONValue) -> DefaultColumnTransformation:
        self._visible = default_col.get("visible", True)
        self._normalize = default_col.get("normalize", True)

        return DefaultColumnTransformation(self._visible, self._normalize)


class ColumnTransformationParser:
    _logger = logging.getLogger(__name__)

    def __init__(self, risky: bool,
                 binop_by_name, prefix_unop_by_name, infix_unop_by_name,
                 func_by_type, func_by_agg,
                 default_column_transformation: DefaultColumnTransformation):
        self._risky = risky
        self._binop_by_name = binop_by_name
        self._prefix_unop_by_name = prefix_unop_by_name
        self._infix_unop_by_name = infix_unop_by_name
        self._func_by_agg = func_by_agg
        self._func_by_type = func_by_type
        self._default_column_transformation = default_column_transformation
        self._visible = default_column_transformation.is_visible()
        self._type = id_func
        self._filter = true_func
        self._map = id_func
        self._agg = cast(Optional[ColAgg], None)
        self._rename = default_column_transformation.rename
        self._id = self._rename

    def parse(self, json_col: JSONValue):
        try:
            self._visible = json_col["visible"]
        except KeyError:
            pass

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

        try:
            col_agg_str = json_col["agg"]
        except KeyError:
            pass
        else:
            self._parse_col_agg(col_agg_str)

        return ColumnTransformation(
            self._visible, self._type, self._filter, self._map, self._agg,
            self._rename, self._id,
        )

    def _parse_col_type(self, col_type_str: str):
        try:
            self._type = self._func_by_type[col_type_str]
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
            parser = ExpressionParser(self._binop_by_name,
                                      self._prefix_unop_by_name,
                                      self._infix_unop_by_name)
        return parser

    def _parse_col_map(self, col_map_str: str):
        parser = self._get_parser()
        self._map = parser.parse(col_map_str)

    def _parse_col_agg(self, col_agg_str: str):
        try:
            self._agg = self._func_by_agg[col_agg_str]
        except KeyError:
            self._logger.exception("Agg error")

    def _parse_col_rename(self, col_rename: str):
        self._rename = lambda _name: col_rename

    def _parse_col_id(self, col_id: str):
        self._id = lambda _name: col_id


class TransformationParser:
    _logger = logging.getLogger(__name__)

    def __init__(self, risky: bool, func_by_type, func_by_agg, binop_by_name,
                 prefix_unop_by_name, infix_unop_by_name):
        self._risky = risky
        self._func_by_type = func_by_type
        self._func_by_agg = func_by_agg
        self._binop_by_name = binop_by_name
        self._prefix_unop_by_name = prefix_unop_by_name
        self._infix_unop_by_name = infix_unop_by_name
        self._main_filter = true_func
        self._default_column_transformation = DefaultColumnTransformation(True,
                                                                          False)
        self._col_transformation_by_name = cast(Dict[str, ColumnTransformation],
                                                {})
        self._extra_prefix = "extra"
        self._extra_count = 1024

    def parse(self, json_transformation: JSONValue) -> Transformation:
        try:
            self._parse_main_filter(json_transformation["main_filter"])
        except KeyError:
            pass
        default_col = json_transformation.get("default_col", {})
        self._parse_default_col(default_col)

        cols = json_transformation.get("cols", {})
        for name, col in cols.items():
            self._col_transformation_by_name[
                name] = ColumnTransformationParser(
                self._risky,
                self._binop_by_name, self._prefix_unop_by_name,
                self._infix_unop_by_name,
                self._func_by_type, self._func_by_agg,
                self._default_column_transformation).parse(col)

        extra = json_transformation.get("extra", {})
        self._parse_extra(extra)

        return Transformation(self._main_filter,
                              self._default_column_transformation,
                              self._col_transformation_by_name,
                              self._extra_prefix, self._extra_count)

    def _parse_main_filter(self, main_filter_str: str):
        if self._risky:
            self._main_filter = RiskyMainFilterParser().parse(main_filter_str)
        else:
            self._main_filter = MainFilterParser(
                self._binop_by_name, self._prefix_unop_by_name,
                self._infix_unop_by_name).parse(main_filter_str)

    def _parse_default_col(self, defaut_col: JSONValue):
        self._default_column_transformation = DefaultColumnTransformationParser().parse(
            defaut_col)

    def _parse_extra(self, extra: JSONValue):
        self._extra_count = extra.get("count", -1)
        self._extra_prefix = extra.get("prefix", "extra")


SUFFIX_REGEX = re.compile(r"^(.*)_(\d+)$")


def improve_header(header: List[str]) -> List[str]:
    c = collections.Counter(header)
    duplicates = set(k for k, v in c.items() if v > 1)
    seen = set(header)
    new_header = []
    for f in header:
        f = f.strip()
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


def add_fields(fields, prefix="extra", total=1024):
    if len(fields) >= total:
        return fields

    return fields + ["{}_{}".format(prefix, i) for i in
                     range(1, total - len(fields) + 1)]
