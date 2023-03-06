"""
A deser module for VW files. See https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Input-format for format.

No extra dependency needed, those are plain text files
"""

# NOTE we are using pyrsistent here for speedup and glamour. Unless issue discovered, let's use it everywhere
# NOTE we are parsing vw rows by hand. Ideally replace by existing parser or rewrite to pyparsing etc

import logging
from dataclasses import dataclass
from functools import reduce
from typing import Iterable, Optional

from fsspec.spec import AbstractFileSystem
from pyrsistent import pvector
from pyrsistent.typing import PVector
from typing_extensions import Self

from fsql.deser import DataReader, PartitionReadFailure, PartitionReadOutcome
from fsql.partition import Partition

logger = logging.getLogger(__name__)


class VwParsingError(ValueError):
    pass


@dataclass(frozen=True, eq=True)
class FeatureNamespace:
    name: str
    scaling: float

    @classmethod
    def from_str(cls, raw: str) -> Self:
        head, *tail = raw.split(":")
        if len(tail) > 1:
            raise VwParsingError(f"unparseable namespace {raw}")
        if len(tail) == 0:
            scaling = 1.0
        else:
            scaling = float(tail[0])
        return cls(name=head, scaling=scaling)


@dataclass
class Feature:
    name: str
    value: Optional[float]

    @classmethod
    def from_str(cls, raw: str) -> Self:
        head, *tail = raw.split(":")
        if len(tail) > 1:
            raise VwParsingError(f"unparseable feature {raw}")
        if len(tail) == 0:
            value = None
        else:
            value = float(tail[0])
        return cls(name=head, value=value)


@dataclass
class VwRow:
    label: float
    importance: float
    tag: Optional[str]
    features: dict[FeatureNamespace, list[Feature]]

    @classmethod
    def from_str(cls, raw: str) -> Self:
        header_raw, *namespaces = raw.split("|")
        header_data = header_raw.strip().split(" ")
        label = float(header_data.pop(0))
        if (not header_raw.endswith(" ")) or (header_data and header_data[-1].startswith("'")):
            tag = header_data.pop(-1).strip().lstrip("'")
        else:
            tag = None
        if len(header_data) > 1:
            raise VwParsingError(f"unparseable {header_data=}, {header_raw=}")
        if len(header_data) == 1:
            importance = float(header_data[0])
        else:
            importance = 1.0
        features = {}
        for namespace_raw in namespaces:
            namespace_elements = namespace_raw.strip().split(" ")
            if not namespace_raw.startswith(" "):
                namespace = FeatureNamespace.from_str(namespace_elements.pop(0).strip())
            else:
                namespace = FeatureNamespace("", 1.0)
            if namespace in features:
                raise VwParsingError(f"duplicate namespace {namespace}")
            features[namespace] = [Feature.from_str(e.strip()) for e in namespace_elements]
        return cls(label=label, importance=importance, tag=tag, features=features)


class VwReader(DataReader[PVector[VwRow]]):
    def read_single(self, partition: Partition, fs: AbstractFileSystem) -> PartitionReadOutcome:
        logger.debug(f"read single for partition {partition}")
        # TODO support for input format, AUTO, etc? Or just some assert-raise?

        def _read_internal(partition: Partition) -> PartitionReadOutcome:
            with fs.open(partition.url, "r") as fd:
                try:
                    return [pvector().extend(VwRow.from_str(line.strip()) for line in fd)], []
                except VwParsingError as e:
                    if not self.lazy_errors:
                        raise
                    else:
                        return pvector(), [PartitionReadFailure(partition, str(e))]

        try:
            return _read_internal(partition)
        except FileNotFoundError as e:
            logger.warning(f"file {partition} reading exception {type(e)}, attempting cache invalidation and reread")
            fs.invalidate_cache()
            return _read_internal(partition)

    def concat(self, data: Iterable[PVector[VwRow]]) -> PVector:
        return reduce(lambda acc, e: acc + e, data, pvector())
