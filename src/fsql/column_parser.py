"""Captures how the filesystem path is translated into table *partition* columns.

There are two standard formats: colName1=val/colName2=val (e.g., year=2022/month=10/...),
which we call Auto, and val/val (e.g., 2022/10) which we call FixedColumns. More arcane
approaches can be handled by implementing the interface `ColumnParser`, which is part of this
module's public API.

In more formal words, `ColumnParser` describes the _grammar_ of the table's partition columns, because,
for optimisation reasons, it also has `generate` method -- be sure to use it whenever possible,
so that the `partition_discovery` module can exploit this.

The standard usage of this module should consist of constructing instances of the AutoParser or
FixedColumns parser using the `from_str` methods -- see their individual documentations.
"""

# TODO
# This may better be named 'Table' or 'TableContract', or even PartitionsGrammar. In that case, the __call__ method
# would rather have an explicit parse name.
# More importantly, the whole class hierarchy in here is wrong. We have a single ColumnParser class with two
# implementations. But there are actually four separate concerns:
# - grammar of the column partitions (includes both parser and generator)
# - pre-specifying some partition values (ie, providing input for generator)
# - exhaustiveness of parsing (early stopping or detection thereof)
# - whether filenames are parsed
# And covering that with just one hierarchy is not a good idea. Instead, we should make this more fine-grained,
# with a number of pre-configured classes to choose from (and possibly configure them further).
# This will get us rid of that weird calling of init of an abstract class.

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from fsql.partition import Partition

logger = logging.getLogger(__name__)


@dataclass
class PartitionGrammar:  # this is a quite bad name
    name: str
    values: Optional[list[str]]


class ColumnParser(ABC):
    @abstractmethod
    def __call__(self, dirname: str) -> tuple[str, str]:
        raise NotImplementedError("abc")

    @abstractmethod
    def tail(self, partition: Partition) -> ColumnParser:
        raise NotImplementedError("abc")

    @abstractmethod
    def generate(self) -> Optional[list[str]]:
        raise NotImplementedError("abc")

    @abstractmethod
    def parses_filenames(self) -> bool:
        raise NotImplementedError("abc")

    @abstractmethod
    def is_terminal_level(self) -> bool:
        raise NotImplementedError("abc")

    @classmethod
    def from_str(cls, path_description: str):
        """Example: col1/col2=v1/col3=[v4,v5,v6]/colFname"""
        # NOTE this whole method better be replaced with a proper parser
        def process_single_partition(partition_desc: str):
            eq_split = partition_desc.split("=")
            if len(eq_split) == 1:
                return PartitionGrammar(eq_split[0], None)
            else:
                if eq_split[1][0] == "[":
                    return PartitionGrammar(eq_split[0], eq_split[1][1:-1].split(","))
                else:
                    return PartitionGrammar(eq_split[0], [eq_split[1]])

        # not sure how to correctly handle this -- I don't want to declare the __init__ here abstract...
        return cls([process_single_partition(e) for e in path_description.split("/")])  # type: ignore


class AutoParser(ColumnParser):
    def __init__(self, partition_grammars: Optional[list[PartitionGrammar]] = None):
        self.partitions = partition_grammars

    def __call__(self, dirname: str) -> tuple[str, str]:
        key, value = dirname.strip("/").split("=", 1)
        return key, value  # we don't return directly due to mypy not understanding split(_, 1)

    def tail(self, partition: Partition) -> ColumnParser:
        if not self.partitions:
            return self
        else:
            # TODO performance issue
            # ideally, the pop call would return an existing instance, which would be pre-created...
            return AutoParser(self.partitions[1:])

    def parses_filenames(self) -> bool:
        return False

    def is_terminal_level(self) -> bool:
        # NOTE a quirk -- if partitions not provided, we read files at every stage of crawling, and thus
        # can't guarantee all of them containing the same number of columns. Fixing that would require
        # first iterating through all of the discovered partitions and filtering for max length only.
        # In the same vein, we don't guarantee even that the columns are the same for every partition.
        # For that, however, the best we could do is crash in case of inconsistency...
        if not self.partitions:
            return True
        else:
            return len(self.partitions) == 0

    def generate(self) -> Optional[list[str]]:
        if self.partitions and self.partitions[0].values:
            return [f"{self.partitions[0].name}={value}" for value in self.partitions[0].values]
        else:
            return None


class FixedColumnsParser(ColumnParser):
    def __init__(self, partition_grammars: list[PartitionGrammar]):
        self.partitions = partition_grammars

    def __call__(self, dirname: str) -> tuple[str, str]:
        return (self.partitions[0].name, dirname.strip("/"))

    def tail(self, partition: Partition) -> ColumnParser:
        # TODO performance issue
        # ideally, the pop call would return an existing instance, which would be pre-created...
        return FixedColumnsParser(self.partitions[1:])

    def parses_filenames(self) -> bool:
        return True

    def is_terminal_level(self) -> bool:
        return len(self.partitions) == 1

    def generate(self) -> Optional[list[str]]:
        if not self.partitions:
            raise ValueError("no partitions remaining")
        if self.partitions[0].values:
            return self.partitions[0].values
        else:
            return None


AUTO_PARSER = AutoParser()
