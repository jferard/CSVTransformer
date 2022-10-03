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
import datetime as dt
import decimal
import math
import operator
import random
import re
import statistics

from csv_transformer.functions import to_date, to_datetime, to_date_or_datetime, \
    add_years, add_months, age, case, str_to_float, str_to_date, \
    str_to_datetime, datetime_from_us_format, date_from_us_format, str_to_int, \
    str_to_decimal, to_path, with_stem, with_filename
from csv_transformer.simple_eval import Function, PrefixUnOp, BinOp

BINOP_BY_NAME = {
    f.name: f for f in [
        BinOp(".", 1, False, None),
        BinOp("^", 2, False, operator.pow),
        BinOp("*", 3, True, operator.mul),
        BinOp("/", 3, True, operator.truediv),
        BinOp("%", 3, True, operator.mod),
        BinOp("+", 4, True, operator.add),
        BinOp("-", 4, True, lambda a, b: a - b),
        BinOp("<", 6, True, operator.lt),
        BinOp("<=", 6, True, operator.le),
        BinOp("==", 6, True, operator.eq),
        BinOp("!=", 6, True, operator.ne),
        BinOp(">=", 6, True, operator.ge),
        BinOp(">", 6, True, operator.gt),
        BinOp("and", 11, True, operator.and_),
        BinOp("or", 12, True, operator.or_),
        BinOp("(", 15, False, None),
        BinOp(",", 15, False, None),
        BinOp(")", 15, False, None),
    ]
}

PREFIX_UNOP_BY_NAME = {
    f.name: f for f in [
        # https://www.postgresql.org/docs/current/functions-math.html
        Function("abs", abs),
        Function("ceil", math.ceil),
        Function("div", operator.floordiv),
        Function("exp", math.exp),
        Function("factorial", math.factorial),
        Function("floor", math.floor),
        Function("ln", math.log),
        Function("log2", math.log2),
        Function("log10", math.log10),
        Function("pi", lambda: math.pi),
        Function("round", round),
        Function("sign", lambda x: -1 if x < 0 else 1 if x > 0 else 0),
        Function("sqrt", math.sqrt),

        Function("random", random.random),
        Function("randint", random.randint),

        Function("cos", math.cos),
        Function("sin", math.sin),
        Function("tan", math.tan),
        Function("acos", math.acos),
        Function("asin", math.asin),
        Function("atan", math.atan),

        # https://www.postgresql.org/docs/current/functions-string.html
        Function("format", str.format),
        Function("len", len),
        Function("lower", str.lower),
        Function("upper", str.upper),
        Function("position", str.find),
        Function("substring", lambda s, *args: s[range(*args)]),
        Function("trim", str.strip),

        Function("max", max),
        Function("min", min),
        Function("avg", lambda *args: statistics.mean(args)),

        # https://www.postgresql.org/docs/current/functions-matching.html
        Function("re_match", re.match),
        Function("re_search", re.search),
        Function("re_first", lambda p, s: next(re.finditer(p, s), None)),

        # https://www.postgresql.org/docs/current/functions-formatting.html
        Function("int", int),
        Function("float", float),
        Function("strpdate", lambda s, f: dt.datetime.strptime(s, f).date()),
        Function("strfdate", lambda d, f: dt.datetime.strftime(d, f)),
        Function("strpdatetime", dt.datetime.strptime),
        Function("strfdatetime", dt.datetime.strftime),
        Function("str", str),
        Function("date", to_date),
        Function("datetime", to_datetime),

        # https://www.postgresql.org/docs/current/functions-datetime.html
        Function("add_years",
                 lambda d, y: add_years(to_date_or_datetime(d), y)),
        Function("add_months",
                 lambda d, y: add_months(to_date_or_datetime(d), y)),
        Function("add_days",
                 lambda d, i: to_date_or_datetime(d) + dt.timedelta(days=i)),
        Function("add_hours",
                 lambda d, i: to_datetime(d) + dt.timedelta(hours=i)),
        Function("add_minutes",
                 lambda d, i: to_datetime(d) + dt.timedelta(minutes=i)),
        Function("add_seconds",
                 lambda d, i: to_datetime(d) + dt.timedelta(seconds=i)),
        Function("age", age),
        Function("day", lambda d: to_date(d).day),
        Function("month", lambda d: to_date(d).month),
        Function("year", lambda d: to_date(d).year),
        Function("hour", lambda d: to_datetime(d).hour),
        Function("minute", lambda d: to_datetime(d).minute),
        Function("second", lambda d: to_datetime(d).second),

        # https://www.postgresql.org/docs/current/functions-conditional.html
        Function("if", lambda x, y, z: y if x else z),
        Function("case", case),

        # path
        Function("path", to_path),
        Function("stem", lambda p: to_path(p).stem),
        Function("suffix", lambda p: to_path(p).suffix),
        Function("dir", lambda p: to_path(p).parent),
        Function("with_suffix", lambda p, s: to_path(p).with_suffix(s)),
        Function("with_stem", with_stem),
        Function("with_filename", with_filename),

        # ops
        PrefixUnOp("-", 2, False, operator.neg),
        PrefixUnOp("!", 2, False, operator.not_),
    ]
}

# see https://en.wikipedia.org/wiki/Order_of_operations#Programming_languages
INFIX_UNOP_BY_NAME = {}

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
    "string_agg": lambda xs: ", ".join(map(str, xs)),
    "d_string_agg": lambda xs: ", ".join(set(map(str, xs))),
    "o_string_agg": lambda xs: ", ".join(sorted(map(str, xs))),
    "do_string_agg": lambda xs: ", ".join(sorted(set(map(str, xs)))),
}

FUNC_BY_TYPE = {
    "int": str_to_int,
    "float": str_to_float,
    "float_us": float,
    "decimal": str_to_decimal,
    "decimal_us": decimal.Decimal,
    "date": str_to_date,
    "date_us": date_from_us_format,
    "datetime": str_to_datetime,
    "datetime_us": datetime_from_us_format
}
