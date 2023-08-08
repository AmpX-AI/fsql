"""Handles filtering of the partitions prior to being read, based on values of partition columns.

TODO Document more:
 - factory-based approach
 - partial query evaluation
"""
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional

# TODO
# - query should impl some caching mechanism for partitions, to not re-eval over and over
# - query could impl an optional "generate" method and have an ColEq factory method,
#   to speed up discovery with conditions like 'day=13'. This requires auto column parser
#   to allow for column order specs


class Query(ABC):
    @abstractmethod
    def eval_all(self, columns: dict[str, str]) -> bool:
        raise NotImplementedError("abc")

    @abstractmethod
    def eval_available(self, columns: dict[str, str]) -> bool:
        raise NotImplementedError("abc")


class BooleanOperatorQuery(Query):
    def __init__(self, left: Query, right: Query, operator: Callable[[bool, bool], bool]):
        self.left = left
        self.right = right
        self.operator = operator

    def eval_all(self, columns: dict[str, str]) -> bool:
        return self.operator(self.left.eval_all(columns), self.right.eval_all(columns))

    def eval_available(self, columns: dict[str, str]) -> bool:
        return self.operator(self.left.eval_available(columns), self.right.eval_available(columns))


class AtomicQuery(Query):
    def __init__(self, f: Callable, columns: Optional[set[str]] = None):
        self.f = f
        if columns:  # TODO this is to impl the Q_EQ, feels hacky
            self.columns = columns
        else:
            args = inspect.getfullargspec(f).args
            self.columns = set(args)

    def eval_all(self, columns: dict[str, str]) -> bool:
        if not self.columns.issubset(columns.keys()):
            return False
        return self.f(**{key: val for key, val in columns.items() if key in self.columns})

    def eval_available(self, columns: dict[str, str]) -> bool:
        if not self.columns.issubset(columns.keys()):
            return True
        return self.f(**{key: val for key, val in columns.items() if key in self.columns})


# NOTE for some reason, the following lines were not understood by mypyc
# thus we reimplement via usual functions
# Q_AND = lambda l, r: BooleanOperatorQuery(l, r, lambda lr, rr: lr and rr)  # noqa: E731
# Q_OR = lambda l, r: BooleanOperatorQuery(l, r, lambda lr, rr: lr or rr)  # noqa: E731


def Q_AND(l: Query, r: Query) -> Query:  # noqa: E741
    return BooleanOperatorQuery(l, r, lambda lr, rr: lr and rr)


def Q_OR(l: Query, r: Query) -> Query:  # noqa: E741
    return BooleanOperatorQuery(l, r, lambda lr, rr: lr or rr)


def Q_EQ(column: str, value: str):
    def f(**kwargs):  # TODO how to define f(`column`) ? That would simplify AtomicQuery then
        return kwargs[column] == value

    return AtomicQuery(f, set([column]))


class ConstantQuery(Query):
    def __init__(self, value: bool):
        self.constant = value

    def eval_all(self, columns: dict[str, str]) -> bool:
        return self.constant

    def eval_available(self, columns: dict[str, str]) -> bool:
        return self.constant


Q_TRUE = ConstantQuery(True)
Q_FALSE = ConstantQuery(False)


class ColumnComparator(Enum):
    lex = auto()
    num = auto()
    wld = auto()

    def compare(self, a: str, b: str) -> int:
        if self is ColumnComparator.wld:
            return 0
        elif self is ColumnComparator.num:
            return int(a) - int(b)
        elif self is ColumnComparator.lex:
            if a < b:
                return -1
            elif a == b:
                return 0
            else:
                return 1


@dataclass
class ColumnRange:
    name: str
    min_value: str
    max_value: str
    column_comparator: ColumnComparator = field(default=ColumnComparator.lex)

    def __post_init__(self):
        if self.column_comparator.compare(self.min_value, self.max_value) > 0:
            raise ValueError(f"invalid range: {self}")


class LexRangeQuery(Query):
    """This is a query to return all files that lie >= c1=s1/c2=s2/... but < c1=e1/c2=e2/...
    It is a lexicographical comparator -- if c1<e1, then c2 can be well above e2 but the file is still accepted.
    Similarly, if c1=s1, then c2 can be arbitrarily large, but must not be <s2.
    Beware that the interval is half-open (so >=, but <) -- those are purposefully chosen for their convenient
    algebraic properties -- [p1, p2) + [p2, p3) == [p1, p3).

    Usage of generators is preferred if the range can be explicitly generated (e.g., for Date Columns), but
    in general case this does the job as well. ColumnComparator can be used to distinguish between:
         - wld: a wildcard, any value in the given column satisfies but comparison continues,
         - num: values are treated numerically, that is, 9 < 10. However, all partition values must be `int()`,
         - lex: the default, values are compared lexicographically.

    See tests/test_lex_range_query.py for more examples"""

    def __init__(self, ranges: list[ColumnRange]):
        self.ranges = ranges

    def _eval_generic(self, columns: dict[str, str], on_early_stop: bool) -> bool:
        at_minimum = False
        at_maximum = False
        for c in self.ranges:
            if c.name not in columns:
                return on_early_stop
            if c.column_comparator == ColumnComparator.wld:
                continue
            value = columns[c.name]
            left = c.column_comparator.compare(c.min_value, value)
            right = c.column_comparator.compare(value, c.max_value)
            if (left < 0 or at_maximum) and (right < 0 or at_minimum):
                return True
            if left == 0:
                at_minimum = True
                continue
            if right == 0:
                at_maximum = True
                continue
            return False
        return not at_maximum

    def eval_all(self, columns: dict[str, str]) -> bool:
        return self._eval_generic(columns, False)

    def eval_available(self, columns: dict[str, str]) -> bool:
        return self._eval_generic(columns, True)
