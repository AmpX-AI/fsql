"""Implements handy utilities for querying date ranges:
- DateRangeQuery, if you need to combine it with other Queries or specific column parsing
- DateRangeGenerator, which is a "column parser" useful if you have just ymd table and no other worries

Both have a similar API and behaviour:
- you specify the names of the ymd columns,
- you specify start and end as either string or datetime.date,
- the ranges are treated as [start, end).
"""

from __future__ import annotations

import calendar
import datetime
import logging
from enum import Enum, auto, unique
from typing import Optional, Union

from fsql import assert_exhaustive_enum
from fsql.column_parser import ColumnParser
from fsql.partition import Partition
from fsql.query import Query

logger = logging.getLogger(__name__)


def _date_parser(date_spec: Union[str, datetime.date]) -> datetime.date:
    if isinstance(date_spec, datetime.date):
        return date_spec
    else:
        return datetime.datetime.strptime(date_spec, "%Y/%m/%d").date()


class DateRangeQuery(Query):
    """Allows for selection ymd-partitions that fall into a given range."""

    def __init__(
        self,
        start: Union[str, datetime.date],
        end: Union[str, datetime.date],
        year_name: str = "year",
        month_name: str = "month",
        day_name: str = "day",
    ):
        """Evaluates to true for all ymd-partitions that are in the range [start, end). The range is specified either
        as "yyyy/mm/dd" string, or as date object. The `*_name` params are the target partition columns from which to
        construct the date object."""
        self.map = {"year": year_name, "month": month_name, "day": day_name}
        self.start = _date_parser(start)
        self.end = _date_parser(end)

    def eval_all(self, columns: dict[str, str]) -> bool:
        if not set(self.map.values()).issubset(columns.keys()):
            rv = False
        else:
            extractor = lambda s: columns[self.map[s]]  # noqa: E731
            extracted = f"{extractor('year')}/{extractor('month')}/{extractor('day')}"
            partition_value = _date_parser(extracted)
            rv = partition_value >= self.start and partition_value < self.end
        logger.debug(f"invoked daterange-all with {columns}, resulted to {rv}")
        return rv

    def eval_available(self, columns: dict[str, str]) -> bool:
        # here we need to eval that "this current partition can expand to at least one legit date"
        # we do this by generating the leftmost and rightmost dates, and comparing against end/start
        # in case all ymd are available already, this evaluates identically to eval_all
        if not self.map["year"] in columns:
            rv = True
        else:
            year = int(columns[self.map["year"]])
            month_l = int(columns.get(self.map["month"], "1"))
            month_r = int(columns.get(self.map["month"], "12"))
            day_l = int(columns.get(self.map["day"], "1"))
            day_r = int(columns.get(self.map["day"], str(calendar.monthrange(year, month_r)[1])))
            date_l = datetime.date(year, month_l, day_l)
            date_r = datetime.date(year, month_r, day_r)
            rv = date_l < self.end and date_r >= self.start
        logger.debug(f"invoked daterange-available with {columns}, resulted to {rv}")
        return rv


@unique
class DRLevel(Enum):
    Y = auto()
    M = auto()
    D = auto()
    F = auto()


class DateRangeGenerator(ColumnParser):
    """Use this in place of AutoParser or FixedColumnParser, if you have just ymd columns and want
    a date range partition. Can work with both =- and Fixed format. To determine which to use, see
    the `build` method -- which is the entrypoint you should use."""

    def __init__(
        self,
        start: datetime.date,
        end: datetime.date,
        level: DRLevel,
        map: dict[DRLevel, str],
        include_column_in_path: bool,
    ):
        self.start = start
        self.end = end
        self.level = level
        self.map = map
        self.include_column_in_path = include_column_in_path

    def __call__(self, dirname: str) -> tuple[str, str]:
        if self.include_column_in_path:
            key, value = dirname.strip("/").split("=", 1)
        else:
            key = self.map[self.level]
            value = dirname.strip("/")
        return key, value

    def tail(self, partition: Partition) -> ColumnParser:
        # this is heavy metal -- we need to restrict the start/end range to those valid for the current partition
        year = int(partition.columns[self.map[DRLevel.Y]])
        if self.level is DRLevel.Y:
            start = self.start if year == self.start.year else datetime.date(year, 1, 1)
            end = self.end if year == self.end.year else datetime.date(year, 12, 31)
            next_level = DRLevel.M
        elif self.level is DRLevel.M:
            month = int(partition.columns[self.map[DRLevel.M]])
            start = self.start if month == self.start.month else datetime.date(year, month, 1)
            month_range = calendar.monthrange(year, month)[1]
            end = self.end if month == self.end.month else datetime.date(year, month, month_range)
            next_level = DRLevel.D
        elif self.level is DRLevel.D:
            start = self.start
            end = self.end
            next_level = DRLevel.F
        else:
            raise ValueError("unexpected call of tail -- internal failure to terminate discovery")
        return DateRangeGenerator(start, end, next_level, self.map, self.include_column_in_path)

    def parses_filenames(self) -> bool:
        return False

    def is_terminal_level(self) -> bool:
        return self.level == DRLevel.F

    def generate(self) -> Optional[list[str]]:
        # we add the +1 because we need inclusive ranges
        if self.level is DRLevel.Y:
            int_range = range(self.start.year, self.end.year + 1)
        elif self.level is DRLevel.M:
            int_range = range(self.start.month, self.end.month + 1)
        elif self.level is DRLevel.D:
            int_range = range(self.start.day, self.end.day + 1)
        elif self.level is DRLevel.F:
            return None
        else:
            assert_exhaustive_enum(self.level)
        pref = f"{self.map[self.level]}=" if self.include_column_in_path else ""
        return [f"{pref}{str(e)}" for e in int_range]

    @classmethod
    def from_str(cls, path_description: str):
        raise NotImplementedError(f"class {cls} does not support this method")

    @classmethod
    def build(
        cls,
        start: Union[str, datetime.date],
        end: Union[str, datetime.date],
        year_name: str = "year",
        month_name: str = "month",
        day_name: str = "day",
        include_column_in_path: bool = True,
    ):
        """Generates all ymd-partitions that are in the range [start, end). The range is specified either
        as "yyyy/mm/dd" string, or as date object. The `*_name` params how should the resulting columns be called.
        If you set include_column_in_path, we generate partitions `{year_name}=2022` etc, otherwise just `2022` etc.
        """
        map = {DRLevel.Y: year_name, DRLevel.M: month_name, DRLevel.D: day_name}
        start = _date_parser(start)
        end = _date_parser(end) - datetime.timedelta(1)  # internally, we treat the date as inclusive!
        return cls(start, end, DRLevel.Y, map, include_column_in_path)
