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
import abc
import collections
import logging
import ntpath
import re
from typing import (Mapping, List, Callable, Any, cast, Dict, Iterable,
                    Optional, Iterator, Union)

from csv_transformer.functions import id_func, true_func, normalize, \
    empty_string_func
from csv_transformer.simple_eval import (
    tokenize_expr, evaluate, ShuntingYard)

JSONValue = Union[
    int, float, str, bool, Dict[str, "JSONValue"], List["JSONValue"]]

StrRow = Mapping[str, str]
TypedRow = Mapping[str, Any]
EntityFilter = Callable[[TypedRow], bool]
EntityExpression = Callable[[TypedRow], Any]
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


class BaseColumnTransformation:
    pass


class ColumnTransformation(BaseColumnTransformation):
    def __init__(self, col_id: ColRename, col_visible: bool, col_type: ColType,
                 col_filter: ColFilter, col_map: Expression,
                 col_agg: Optional[ColAgg], col_rename: ColRename):
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


class NewColumnTransformation(BaseColumnTransformation):
    def __init__(self, col_id_str: str, col_visible: bool,
                 col_filter: ColFilter, col_formula: EntityExpression,
                 col_agg: Optional[ColAgg], col_name: str):
        self._col_id_str = col_id_str
        self._visible = col_visible
        self._filter = col_filter
        self._formula = col_formula
        self._agg = col_agg
        self._name = col_name

    def has_agg(self) -> bool:
        return self._agg is not None

    def is_visible(self) -> bool:
        return self._visible

    def agg(self, values: List[Any]) -> Any:
        return self._agg(values)

    def get_id(self) -> str:
        return self._col_id_str

    def col_filter(self, value: Any) -> bool:
        return self._filter(value)

    def col_formula(self, typed_value_by_name: TypedRow) -> Any:
        return self._formula(typed_value_by_name)

    def name(self):
        return self._name


class Transformation:
    def __init__(self, entity_filter: EntityFilter, agg_filter: EntityFilter,
                 default_column_transformation: DefaultColumnTransformation,
                 col_transformation_by_name: Dict[str, ColumnTransformation],
                 new_col_transformations: List[NewColumnTransformation],
                 extra_prefix: str, extra_count: int):
        self._entity_filter = entity_filter
        self._agg_filter = agg_filter
        self._default_column_transformation = default_column_transformation
        self._col_transformation_by_name = col_transformation_by_name
        self._new_col_transformations = new_col_transformations
        self._col_transformation_by_id = {
            self._col_id(n): v for n, v in col_transformation_by_name.items()
        }
        self._new_col_transformation_by_id = {
            nt.get_id(): nt for nt in new_col_transformations
        }
        self._extra_prefix = extra_prefix
        self._extra_count = extra_count
        self._d = {}

    def has_agg(self) -> bool:  # TODO
        return any(self._col_is_agg(col_id)
                   for col_id in self._col_transformation_by_id)

    def add_fields(self, header: List[str]) -> List[str]:
        if self._extra_count > 0:
            return add_fields(header, self._extra_prefix, self._extra_count)
        else:
            return header

    def file_header(self, header: Iterable[str]) -> List[str]:
        h = [self._col_rename(n) for n in header if
             self._col_is_visible_by_name(n)]
        h += [nt.name() for nt in self._new_col_transformations if
              self._new_col_is_visible_by_id(nt.get_id())]
        return h

    def col_ids(self, header: Iterable[str]) -> List[Optional[str]]:
        return [self._col_id(n) for n in header] + [nt.get_id() for nt in
                                                    self._new_col_transformations]

    def visible_col_ids(self, header: Iterable[str]) -> List[Optional[str]]:
        return [self._col_id(n) for n in header
                if self._col_is_visible_by_name(n)] + [nt.get_id() for nt in
                                                       self._new_col_transformations
                                                       if nt.is_visible()]

    def transform(self, value_by_id: StrRow) -> Optional[TypedRow]:
        typed_value_by_id = self._type_row(value_by_id)
        typed_value_by_id = self._extend_row(typed_value_by_id)
        typed_value_by_id = self._map(typed_value_by_id)
        # add new cols
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
        if self._entity_filter(typed_value_by_id):
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
        """for agg"""
        value_by_id = self._type_row(value_by_id)
        value_by_id = self._extend_row(value_by_id)
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

    def agg_filter(self, value_by_id: TypedRow) -> bool:
        return self._agg_filter(value_by_id)

    def _col_is_agg(self, col_id: str) -> bool:
        t = self._col_transformation_by_id.get(col_id,
                                               self._new_col_transformation_by_id.get(
                                                   col_id))

        if t is None:
            return False

        return t.has_agg()

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

    def _new_col_is_visible_by_id(self, col_id: str) -> bool:
        try:
            return self._new_col_transformation_by_id[col_id].is_visible()
        except KeyError:
            return self._default_column_transformation.is_visible()

    def _col_id(self, name: str) -> str:
        try:
            return self._col_transformation_by_name[name].get_id(name)
        except KeyError:
            return self._default_column_transformation.rename(name)

    # def _new_col_id(self, name: str) -> str:  # TODO: check
    #     try:
    #         return self._new_col_transformation_by_name[name].get_id(name)
    #     except KeyError:
    #         return self._default_column_transformation.rename(name)

    def _col_is_visible_by_id(self, col_id: str) -> bool:
        try:
            return self._col_transformation_by_id[col_id].is_visible()
        except KeyError:
            return self._default_column_transformation.is_visible()

    def _col_filter(self, col_id: str, value: Any) -> bool:
        t = self._col_transformation_by_id.get(
            col_id, self._new_col_transformation_by_id.get(col_id))

        if t is None:
            return True

        return t.col_filter(value)

    def _col_map(self, col_id: str, value: Any) -> Any:
        try:
            return self._col_transformation_by_id[col_id].col_map(value)
        except KeyError:
            return value

    def _extend_row(self, typed_value_by_id: TypedRow) -> TypedRow:
        for new_col in self._new_col_transformations:
            typed_value_by_id[new_col.get_id()] = new_col.col_formula(
                typed_value_by_id)
        return typed_value_by_id


class EntityFilterParser:
    _logger = logging.getLogger(__name__)

    def __init__(self, binop_by_name, prefix_unop_by_name,
                 infix_unop_by_name):
        self.binop_by_name = binop_by_name
        self.prefix_unop_by_name = prefix_unop_by_name
        self.infix_unop_by_name = infix_unop_by_name

    def parse(self, entity_filter_str: str) -> EntityFilter:
        tokens = tokenize_expr(entity_filter_str)
        tokens = ShuntingYard(False, self.binop_by_name,
                              self.prefix_unop_by_name,
                              self.infix_unop_by_name).process(tokens)
        return lambda r: evaluate(tokens, r)


class RiskyEntityFilterParser:
    _logger = logging.getLogger(__name__)

    def parse(self, entity_filter_str: str) -> EntityFilter:
        return lambda r: eval(entity_filter_str, {}, r)


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


class BaseColumnTransformationBuilder(abc.ABC):
    _logger = logging.getLogger(__name__)

    def __init__(self, transformation_builder: "TransformationBuilder",
                 default_column_transformation: DefaultColumnTransformation):
        self._transformation_builder = transformation_builder
        self._default_column_transformation = default_column_transformation

    def _parse_col_visible(self, col_visible: Optional[bool]) -> bool:
        if col_visible is None:
            return self._default_column_transformation.is_visible()
        else:
            return col_visible

    def _parse_col_filter(self, col_filter_str: Optional[str]) -> ColFilter:
        if col_filter_str is None:
            return true_func

        parser = self._transformation_builder.expression_parser()
        return parser.parse(col_filter_str)

    def _parse_col_agg(self, col_agg_str: Optional[str]) -> Optional[ColAgg]:
        if col_agg_str is None:
            return None

        try:
            return self._transformation_builder.func_by_agg(col_agg_str)
        except KeyError:
            self._logger.exception("Agg error")
            return None


class ColumnTransformationBuilder(BaseColumnTransformationBuilder):
    _logger = logging.getLogger(__name__)

    def build(self, col_id_str: str, col_visible: Optional[bool],
              col_type_str: str, col_filter_str: str, col_map_str: str,
              col_rename_str: str, col_agg_str: str):

        col_rename = self._parse_col_rename(col_rename_str)
        if col_id_str is None:
            col_id = col_rename
        else:
            col_id = self._parse_col_id(col_id_str)
        col_visible = self._parse_col_visible(col_visible)
        col_type = self._parse_col_type(col_type_str)
        col_filter = self._parse_col_filter(col_filter_str)
        col_map = self._parse_col_map(col_map_str)
        col_agg = self._parse_col_agg(col_agg_str)

        return ColumnTransformation(col_id, col_visible, col_type, col_filter,
                                    col_map, col_agg, col_rename)

    def _parse_col_id(self, col_id_str: str) -> ColRename:
        return lambda _name: col_id_str

    def _parse_col_type(self, col_type_str: Optional[str]) -> ColType:
        if col_type_str is None:
            return id_func

        try:
            return self._transformation_builder.func_by_type(col_type_str)
        except KeyError:
            parser = self._transformation_builder.expression_parser()
            return parser.parse(col_type_str)

    def _parse_col_map(self, col_map_str: str) -> Expression:
        if col_map_str is None:
            return id_func

        parser = self._transformation_builder.expression_parser()
        return parser.parse(col_map_str)

    def _parse_col_rename(self, col_rename: Optional[str]) -> ColRename:
        if col_rename is None:
            return self._default_column_transformation.rename

        return lambda _name: col_rename


class NewColumnTransformationBuilder(BaseColumnTransformationBuilder):
    _logger = logging.getLogger(__name__)

    def build(self, col_id_str: str, col_visible: Optional[bool],
              col_filter_str: Optional[str], col_formula_str: Optional[str],
              col_name_str: Optional[str], col_agg_str: Optional[str]):
        col_id = col_id_str
        col_visible = self._parse_col_visible(col_visible)
        col_filter = self._parse_col_filter(col_filter_str)
        col_formula = self._parse_col_formula(col_formula_str)
        if col_name_str is None:
            col_name = col_id_str
        else:
            col_name = col_name_str
        col_agg = self._parse_col_agg(col_agg_str)

        return NewColumnTransformation(col_id, col_visible, col_filter,
                                       col_formula, col_agg, col_name)

    def _parse_col_formula(self, col_formula_str: str) -> Expression:
        parser = self._transformation_builder.row_filter_parser()
        return parser.parse(col_formula_str)

    def _parse_col_rename(self, col_rename: str):
        self._rename = lambda _name: col_rename


# class ColumnTransformationParser:
#     _logger = logging.getLogger(__name__)
#
#     def __init__(self, transformation_builder: "TransformationBuilder",
#                  default_column_transformation: DefaultColumnTransformation):
#         self._transformation_builder = transformation_builder
#         self._default_column_transformation = default_column_transformation
#         self._col_visible = default_column_transformation.is_visible()
#         self._type = id_func
#         self._filter = true_func
#         self._map = id_func
#         self._agg = cast(Optional[ColAgg], None)
#         self._rename = default_column_transformation.rename
#         self._id = self._rename
#
#     def parse(self, json_col: JSONValue):
#
#         self._transformation_builder.column()
#
#     def _parse_col_type(self, col_type_str: str):
#         try:
#             self._type = self._transformation_builder.func_by_type(col_type_str)
#         except KeyError:
#             parser = self._transformation_builder.expression_parser()
#             self._type = parser.parse(col_type_str)
#
#     def _parse_col_filter(self, col_filter_str: str):
#         parser = self._transformation_builder.expression_parser()
#         self._filter = parser.parse(col_filter_str)
#
#     def _parse_col_map(self, col_map_str: str):
#         parser = self._transformation_builder.expression_parser()
#         self._map = parser.parse(col_map_str)
#
#     def _parse_col_agg(self, col_agg_str: str):
#         try:
#             self._agg = self._transformation_builder.func_by_agg(col_agg_str)
#         except KeyError:
#             self._logger.exception("Agg error")
#
#     def _parse_col_rename(self, col_rename: str):
#         self._rename = lambda _name: col_rename
#
#     def _parse_col_id(self, col_id_str: str):
#         self._id = lambda _name: col_id_str


# class NewColumnTransformationParser:
#     _logger = logging.getLogger(__name__)
#
#     def __init__(self, parser_factory: "TransformationBuilder",
#                  default_column_transformation: DefaultColumnTransformation):
#         self._parser_factory = parser_factory
#         self._default_column_transformation = default_column_transformation
#         self._visible = default_column_transformation.is_visible()
#         self._filter = true_func
#         self._formula = empty_string_func
#         self._agg = cast(Optional[ColAgg], None)
#         self._rename = default_column_transformation.rename
#         self._id = self._rename
#
#     def parse(self, json_col: JSONValue):
#         try:
#             self._visible = json_col["visible"]
#         except KeyError:
#             pass
#
#         try:
#             col_filter_str = json_col["filter"]
#         except KeyError:
#             pass
#         else:
#             self._parse_col_filter(col_filter_str)
#
#         try:
#             col_formula_str = json_col["formula"]
#         except KeyError:
#             pass
#         else:
#             self._parse_col_formula(col_formula_str)
#
#         try:
#             col_rename_str = json_col["rename"]
#         except KeyError:
#             pass
#         else:
#             self._parse_col_rename(col_rename_str)
#
#         try:
#             col_id_str = json_col["id"]
#         except KeyError:
#             pass
#         else:
#             self._parse_col_id(col_id_str)
#
#         try:
#             col_agg_str = json_col["agg"]
#         except KeyError:
#             pass
#         else:
#             self._parse_col_agg(col_agg_str)
#
#         return NewColumnTransformation(
#             self._visible, self._filter, self._formula, self._agg,
#             self._rename, self._id)
#
#     def _parse_col_type(self, col_type_str: str):
#         try:
#             self._type = self._parser_factory.func_by_type(col_type_str)
#         except KeyError:
#             parser = self._parser_factory.expression_parser()
#             self._type = parser.parse(col_type_str)
#
#     def _parse_col_filter(self, col_filter_str: str):
#         parser = self._parser_factory.expression_parser()
#         self._filter = parser.parse(col_filter_str)
#
#     def _parse_col_formula(self, col_formula_str: str):
#         parser = self._parser_factory.expression_parser()
#         self._map = parser.parse(col_formula_str)
#
#     def _parse_col_agg(self, col_agg_str: str):
#         try:
#             self._agg = self._parser_factory.func_by_agg(col_agg_str)
#         except KeyError:
#             self._logger.exception("Agg error")
#
#     def _parse_col_rename(self, col_rename: str):
#         self._rename = lambda _name: col_rename
#
#     def _parse_col_id(self, col_id_str: str):
#         self._id = lambda _name: col_id_str


class TransformationBuilder:
    def __init__(self, risky: bool, func_by_type, func_by_agg, binop_by_name,
                 prefix_unop_by_name, infix_unop_by_name):
        self._risky = risky
        self._func_by_type = func_by_type
        self._func_by_agg = func_by_agg
        self._binop_by_name = binop_by_name
        self._prefix_unop_by_name = prefix_unop_by_name
        self._infix_unop_by_name = infix_unop_by_name
        self._entity_filter = cast(Expression, true_func)
        self._agg_filter = cast(EntityFilter, true_func)
        self._default_column_transformation = cast(
            Optional[DefaultColumnTransformation], None)
        self._col_transformation_by_name = cast(Dict[str, ColumnTransformation],
                                                {})
        self._new_col_transformations = cast(
            List[NewColumnTransformation], [])
        self._extra_prefix = "extra"
        self._extra_count = 1024

    def func_by_type(self, col_type_str: str) -> ColType:
        return self._func_by_type[col_type_str]

    def func_by_agg(self, col_agg_str: str) -> ColAgg:
        return self._func_by_agg[col_agg_str]

    def row_filter_parser(self) -> Union[
        RiskyEntityFilterParser, EntityFilterParser]:
        if self._risky:
            return RiskyEntityFilterParser()
        else:
            return EntityFilterParser(
                self._binop_by_name, self._prefix_unop_by_name,
                self._infix_unop_by_name)

    def expression_parser(self) -> Union[
        RiskyExpressionParser, ExpressionParser]:
        if self._risky:
            return RiskyExpressionParser()
        else:
            return ExpressionParser(self._binop_by_name,
                                    self._prefix_unop_by_name,
                                    self._infix_unop_by_name)

    def build(self):
        return Transformation(self._entity_filter, self._agg_filter,
                              self._default_column_transformation,
                              self._col_transformation_by_name,
                              self._new_col_transformations,
                              self._extra_prefix, self._extra_count)

    def add_col(self, name: str, col_id_str: str, col_visible: bool,
                col_type_str: str, col_filter_str: str, col_map_str: str,
                col_rename_str: str, col_agg_str: str):
        builder = ColumnTransformationBuilder(self,
                                              self._default_column_transformation)
        self._col_transformation_by_name[name] = builder.build(col_id_str,
                                                               col_visible,
                                                               col_type_str,
                                                               col_filter_str,
                                                               col_map_str,
                                                               col_rename_str,
                                                               col_agg_str)

    def add_new_col(self, col_visible: bool,
                    col_filter_str: str, col_formula_str: str,
                    col_rename_str: str,
                    col_id_str: str, col_agg_str: str):
        builder = NewColumnTransformationBuilder(self,
                                                 self._default_column_transformation)
        self._new_col_transformations.append(builder.build(
            col_id_str, col_visible, col_filter_str, col_formula_str,
            col_rename_str, col_agg_str))

    def entity_filter(self, entity_filter_str: str):
        parser = self.row_filter_parser()
        parse = parser.parse(entity_filter_str)
        self._entity_filter = parse

    def agg_filter(self, agg_filter_str: str):
        parser = self.row_filter_parser()
        self._agg_filter = parser.parse(agg_filter_str)

    def default_col(self, visible: True, normalize: True):
        self._default_column_transformation = DefaultColumnTransformation(
            visible, normalize)

    def extra(self, extra_count: int, extra_prefix: str):
        self._extra_count = extra_count
        self._extra_prefix = extra_prefix


class TransformationJsonParser:
    _logger = logging.getLogger(__name__)

    def __init__(self, transformation_builder: "TransformationBuilder"):
        self._transformation_builder = transformation_builder
        # self._entity_filter = true_func
        # self._agg_filter = true_func
        # self._default_column_transformation = DefaultColumnTransformation(
        #     True, False)
        # self._col_transformation_by_name = cast(Dict[str, ColumnTransformation],
        #                                         {})
        # self._new_col_transformation_by_id = cast(
        #     Dict[str, NewColumnTransformation],
        #     {})

    def parse(self, json_transformation: JSONValue) -> Transformation:
        try:
            self._parse_entity_filter(json_transformation["entity_filter"])
        except KeyError:
            pass

        try:
            self._parse_agg_filter(json_transformation["agg_filter"])
        except KeyError:
            pass

        default_col = json_transformation.get("default_col", {})
        self._parse_default_col(default_col)

        cols = json_transformation.get("cols", {})
        self._parse_cols(cols)

        new_cols = json_transformation.get("new_cols", [])
        self._parse_new_cols(new_cols)

        extra = json_transformation.get("extra", {})
        self._parse_extra(extra)

        return self._transformation_builder.build()

    def _parse_entity_filter(self, entity_filter_str: str):
        self._transformation_builder.entity_filter(entity_filter_str)

    def _parse_agg_filter(self, agg_filter_str: str):
        self._transformation_builder.agg_filter(agg_filter_str)

    def _parse_cols(self, cols):
        for name, json_col in cols.items():
            col_id_str = json_col.get("id", None)
            col_visible = json_col.get("visible", None)
            col_type_str = json_col.get("type", None)
            col_filter_str = json_col.get("filter", None)
            col_map_str = json_col.get("map", None)
            col_rename_str = json_col.get("rename", None)
            col_agg_str = json_col.get("agg", None)

            self._transformation_builder.add_col(name, col_id_str, col_visible,
                                                 col_type_str, col_filter_str,
                                                 col_map_str, col_rename_str,
                                                 col_agg_str)

    def _parse_new_cols(self, new_cols):
        for new_col in new_cols:
            col_id_str = new_col.get("id", None)
            col_visible = new_col.get("visible", None)
            col_filter_str = new_col.get("filter", None)
            col_formula_str = new_col.get("formula", None)
            col_rename_str = new_col.get("rename", None)
            col_agg_str = new_col.get("agg", None)

            self._transformation_builder.add_new_col(col_visible,
                                                     col_filter_str,
                                                     col_formula_str,
                                                     col_rename_str,
                                                     col_id_str, col_agg_str)

    def _parse_default_col(self, default_col: JSONValue):
        self._transformation_builder.default_col(
            default_col.get("visible", True),
            default_col.get("normalize", True))

    def _parse_extra(self, extra: JSONValue):
        self._transformation_builder.extra(extra.get("count", -1),
                                           extra.get("prefix", "extra"))


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
