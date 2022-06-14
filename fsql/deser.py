"""Handles conversion of raw bytes + parsed partition columns into formats such as data frames.

The main interface `DataReader` with entrypoint method `read_and_concat` gives the contract of
the operation.  There are the following default instances:
 - PandasReader, which parses to pandas data frames and adds the partition columns as df column,
   and concatenates all the individual sub-frames into a single one
 - EnumeratedDictReader, which reads the data as a dictionary, adds the partition columns as dict
   keys, and concatenates into an enumerated dict (order being the alphabetic of underlying files)

All these autodetect the input format from the suffix of the key. If this is desired to be
overridden with a fixed format, user should instantiate with the desired InputFormat.

If the user desires to implement another format, such as Dask, the `DataReader` interface is the part of
the public API. The main methods are `read_single` (which is executed with a thread pool executor, as
we want to parallelize cloud FS fetches for an acceptable performance) and `concat`. It may happen that
this approach is undesirable due to too-large intermediate representation -- in this case, user should
opt for the lazy approach (such as in Dask), and don't materialize inside neither the `read_single` nor
`concat` methods.

For custom data readers, mind observing the `lazy_errors` variable -- if it is set, the method `read_single`
should not raise upon Partition-specific problems, but accumulate the error instead.

Existing readers such as PandasReader allow customisation via passing through any kwargs to the underlying
pandas read method.

The user should *not* bake in any specific business logic in here -- a more prefered approach is to
return an object such as (lazy) data frame as early as possible, and apply any transformations later on.
"""
from __future__ import annotations

import json
import logging
from abc import abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from enum import Enum, auto, unique
from functools import partial, reduce
from itertools import chain
from typing import Callable, Generic, Iterable, NamedTuple, Tuple, TypeVar, Union

import pandas as pd
from fsspec.spec import AbstractFileSystem

from fsql import assert_exhaustive_enum
from fsql.partition import Partition

logger = logging.getLogger(__name__)

A = TypeVar("A")
B = TypeVar("B")
BiIterator = Tuple[Iterable[A], Iterable[B]]


# lambda acc, e: (chain(acc[0], e[0]), chain(acc[1], e[1])) # not sufficient for mypy
def flatten_biiterator(acc: BiIterator, elem: BiIterator) -> BiIterator:
    """When used with `reduce`, converts an iterable of tuple(iterable, iterable) to a tuple(iterable, iterable)"""
    return (chain(acc[0], elem[0]), chain(acc[1], elem[1]))


@unique
class InputFormat(Enum):
    # NOTE possibly remove this enum as it requires more exhaustive match, and instead use Union[InputFormat, Auto]
    # TODO behaviour for gzipped etc files. Either two dim enum, or concat in here
    AUTO = auto()
    PARQUET = auto()
    JSON = auto()
    CSV = auto()
    XLSX = auto()

    @classmethod
    def from_url(cls, url: str):
        return {
            "json": InputFormat.JSON,
            "parquet": InputFormat.PARQUET,
            "csv": InputFormat.CSV,
            "xlsx": InputFormat.XLSX,
        }[url]


DataObject = TypeVar("DataObject")


class PartitionReadFailure(NamedTuple):
    partition: Partition
    reason: str  # or Any?


# NOTE a namedtuple is preferable, but somehow cannot be cast to BiIterator[A, B]...
# class PartitionReadOutcome(NamedTuple, Generic[DataObject]):
#     data: Iterable[DataObject]
#     failures: Iterable[PartitionReadFailure]
PartitionReadOutcome = Tuple[Iterable[DataObject], Iterable[PartitionReadFailure]]


class DataReader(Generic[DataObject]):
    def __init__(self, input_format: InputFormat = InputFormat.AUTO, lazy_errors=False):
        self.input_format = input_format
        self.lazy_errors = lazy_errors

    def detect_format(self, url: str) -> InputFormat:
        if self.input_format != InputFormat.AUTO:
            return self.input_format
        else:
            return InputFormat.from_url(url.split(".")[-1])

    @abstractmethod
    def read_single(self, partition: Partition, fs: AbstractFileSystem) -> PartitionReadOutcome:
        raise NotImplementedError("abc")

    @abstractmethod
    def concat(self, outcomes: Iterable[DataObject]) -> DataObject:
        raise NotImplementedError("abc")

    def read_and_concat(
        self, partitions: Iterable[Partition], fs: AbstractFileSystem
    ) -> Union[DataObject, Tuple[DataObject, Iterable[PartitionReadFailure]]]:
        # TODO it is profoundly unfortunate that the return type is Union. Ideally, it would be a generic
        # type T that is bound with this union, and determined via lazy_error parameter which would be
        # not a boolean but instead a function [DataObject, T[DataObject]]. Alas, I was not able to pythonize
        # that. Note this would then apply to the api's signature as well
        with ThreadPoolExecutor(max_workers=32) as tpe:  # TODO configurable worker count
            partition_read_outcomes = tpe.map(partial(self.read_single, fs=fs), partitions)
            if self.lazy_errors:
                data_objects, failures = reduce(flatten_biiterator, partition_read_outcomes)
                return (self.concat(data_objects), failures)
            else:
                data_objects = chain(*(e[0] for e in partition_read_outcomes))
                return self.concat(data_objects)


class PandasReader(DataReader[pd.DataFrame]):
    """Wraps various pandas read methods (parquet, json, csv, excel) into a single interface.
    Behaviour can be customised via passing any kwargs to the constructor.
    """

    def __init__(self, input_format=InputFormat.AUTO, lazy_errors=False, **pdread_kwargs):
        super().__init__(input_format=input_format, lazy_errors=lazy_errors)
        self.pdread_user_kwargs = pdread_kwargs
        self.pdread_default_kwargs = defaultdict(dict)
        self.pdread_default_kwargs[InputFormat.PARQUET] = {
            "engine": "fastparquet",
        }
        self.pdread_default_kwargs[InputFormat.JSON] = {
            "lines": "true",
        }
        self.pdread_default_kwargs[InputFormat.XLSX] = {
            "engine": "openpyxl",
        }

    def format_to_reader(self, input_format: InputFormat) -> Callable:
        if input_format is InputFormat.PARQUET:
            return pd.read_parquet
        elif input_format is InputFormat.JSON:
            return pd.read_json
        elif input_format is InputFormat.CSV:
            return pd.read_csv
        elif input_format is InputFormat.XLSX:
            return pd.read_excel
        elif input_format is InputFormat.AUTO:
            raise ValueError("partition had format detected as auto -> invalid state.")
        else:
            assert_exhaustive_enum(input_format)

    def read_single(self, partition: Partition, fs: AbstractFileSystem) -> PartitionReadOutcome:
        logger.debug(f"read dataframe for partition {partition}")
        input_format = self.detect_format(partition.url)
        logger.debug(f"format detected for partition {input_format} <- {partition}")
        reader = self.format_to_reader(input_format)

        pdread_kwargs = {**self.pdread_default_kwargs[input_format], **self.pdread_user_kwargs}
        logger.debug(f"reader kwargs {pdread_kwargs} for partition {partition}")

        def read_dataframe(partition: Partition) -> PartitionReadOutcome:
            with fs.open(partition.url, "rb") as fd:
                try:
                    df = reader(fd, **pdread_kwargs)
                    for key, value in partition.columns.items():
                        df[key] = value
                    return ([df], [])
                except ValueError as e:
                    if not self.lazy_errors:
                        raise
                    else:
                        return ([], [PartitionReadFailure(partition, str(e))])

        try:
            result = read_dataframe(partition)
        except FileNotFoundError as e:
            logger.warning(f"file {partition} reading exception {type(e)}, attempting cache invalidation and reread")
            fs.invalidate_cache()
            result = read_dataframe(partition)

        return result

    def concat(self, data: Iterable[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(data)


PANDAS_READER = PandasReader()


class EnumeratedDictReader(DataReader[dict]):
    def read_single(self, partition: Partition, fs: AbstractFileSystem) -> PartitionReadOutcome:
        logger.debug(f"read single for partition {partition}")
        input_format = self.detect_format(partition.url)
        if input_format != InputFormat.JSON:
            raise ValueError(f"EnumeratedDictReader supports only json, not {input_format}. Partition is {partition}")

        def read_dict(partition: Partition) -> PartitionReadOutcome:
            with fs.open(partition.url, "r") as fd:
                try:
                    base = json.load(fd)
                    return ([{**base, **partition.columns}], [])
                except json.decoder.JSONDecodeError as e:
                    if not self.lazy_errors:
                        raise
                    else:
                        return ([], [PartitionReadFailure(partition, str(e))])

        try:
            result = read_dict(partition)
        except FileNotFoundError as e:
            logger.warning(f"file {partition} reading exception {type(e)}, attempting cache invalidation and reread")
            fs.invalidate_cache()
            result = read_dict(partition)
        return result

    def concat(self, data: Iterable[dict]) -> dict:
        return dict(enumerate(data))


ENUMERATED_DICT_READER = EnumeratedDictReader()
