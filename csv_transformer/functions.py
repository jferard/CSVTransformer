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
import locale
import re
from typing import Union, Optional, Tuple, Any

IntoDate = Union[str, dt.date, dt.datetime]
IntoDatetime = IntoDate


def to_date(v: IntoDate) -> dt.date:
    if isinstance(v, str):
        return dt.date.fromisoformat(v)
    elif isinstance(v, dt.datetime):
        return v.date()
    elif isinstance(v, dt.date):
        return v
    else:
        raise ValueError()


def to_datetime(v: IntoDatetime) -> dt.datetime:
    if isinstance(v, str):
        return dt.datetime.fromisoformat(v)
    elif isinstance(v, dt.datetime):
        return v
    elif isinstance(v, dt.date):
        return dt.datetime(v.year, v.month, v.day)
    else:
        raise ValueError()


def to_date_or_datetime(v: IntoDatetime) -> Union[dt.date, dt.datetime]:
    ret = to_datetime(v)
    if ret.hour == 0 and ret.minute == 0 and ret.second == 0:
        return ret.date()
    else:
        return ret


def add_years(d: dt.date, y: int) -> dt.date:
    if isinstance(d, dt.datetime):
        return dt.datetime(d.year + y, d.month, d.day, d.hour, d.minute,
                           d.second)
    else:
        return dt.date(d.year + y, d.month, d.day)


def add_months(d: dt.date, m: int) -> dt.date:
    if isinstance(d, dt.datetime):
        return dt.datetime(d.year, d.month + m, d.day, d.hour, d.minute,
                           d.second)
    else:
        return dt.date(d.year, d.month + m, d.day)


def age(last: dt.date, first: Optional[dt.date] = None) -> Tuple[int, int, int]:
    """
    We have a first date and a second date.

    :param last:
    :param first:
    :return:
    """
    if first is None:
        first = last
        last = dt.datetime.now().date()
    years = last.year - first.year
    months = last.month - first.month
    days = last.day - first.day
    if days < 0:
        months -= 1
        if last.month == 1:
            middle = dt.date(last.year - 1, 12, first.day)
        else:
            middle = dt.date(last.year, last.month - 1, first.day)
        if isinstance(last, dt.datetime):
            days = (last.date() - middle).days
        else:
            days = (last - middle).days
    if months < 0:
        years -= 1
        months += 12
    if years < 0:
        raise ValueError()

    return years, months, days


def case(*args):
    args_count = len(args)
    assert args_count % 2 == 1
    for i in range(0, args_count - 2, 2):
        if args[i]:
            return args[i + 1]
    return args[args_count - 1]


# TYPE FUNCTIONS #

def str_to_datetime(s: str) -> dt.datetime:
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%y %H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            pass

    try:
        return dt.datetime.fromisoformat(s)
    except:
        return dt.datetime.strptime(s, "%Y-%m-%d")


def str_to_date(s: str) -> dt.date:
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    return dt.date.fromisoformat(s)


def str_to_float(s: str) -> float:
    lconv = locale.localeconv()
    s = s.replace(' ', '')
    s = s.replace(lconv['thousands_sep'], '')
    s = s.replace(lconv['decimal_point'], '.')
    return float(s)


def id_func(x: Any) -> Any: return x


def true_func(_x: Any) -> Any: return True

def empty_string_func(*_x: Any) -> Any: return ""


def normalize(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode(
        'ascii')
    s = SPACE_REGEX.sub("_", s)
    s = s.lower()
    return s


SPACE_REGEX = re.compile(r"\s+")
