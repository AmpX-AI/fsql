"""Handles filtering of the partitions prior to being read, based on values of partition columns.

TODO Document more:
 - factory-based approach
 - partial query evaluation
"""
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
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
