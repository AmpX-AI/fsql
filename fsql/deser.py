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

The user should *not* bake in any specific business logic in here -- a more prefered approach is to
return an object such as data frame as early as possible, and apply any transformations later on.
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from enum import Enum, auto, unique
from functools import partial
from typing import Any

import pandas as pd
from fsspec.spec import AbstractFileSystem

from fsql import assert_exhaustive_enum
from fsql.partition import Partition

logger = logging.getLogger(__name__)


@unique
class InputFormat(Enum):
    # NOTE possibly remove this enum as it requires more exhaustive match, and instead use Union[InputFormat, Auto]
    # TODO behaviour for gzipped etc files. Either two dim enum, or concat in here
    AUTO = auto()
    PARQUET = auto()
    JSON = auto()
    CSV = auto()

    @classmethod
    def from_url(cls, url: str):
        return {"json": InputFormat.JSON, "parquet": InputFormat.PARQUET, "csv": InputFormat.CSV}[url]


class DataReader(ABC):
    def __init__(self, input_format: InputFormat = InputFormat.AUTO):
        self.input_format = input_format

    def detect_format(self, url: str) -> InputFormat:
        if self.input_format != InputFormat.AUTO:
            return self.input_format
        else:
            return InputFormat.from_url(url.split(".")[-1])

    @abstractmethod
    def read_single(self, partition: Partition, fs: AbstractFileSystem) -> Any:  # TODO generics on the Any
        raise NotImplementedError("abc")

    @abstractmethod
    def concat(self, data: Iterable[Any]) -> Any:  # TODO generics on the Any
        raise NotImplementedError("abc")

    def read_and_concat(
        self, partitions: Iterable[Partition], fs: AbstractFileSystem
    ) -> Any:  # TODO generics on the Any
        with ThreadPoolExecutor(max_workers=32) as tpe:  # TODO configurable
            data = tpe.map(partial(self.read_single, fs=fs), partitions)
        return self.concat(data)


class PandasReader(DataReader):
    def read_single(self, partition: Partition, fs: AbstractFileSystem) -> pd.DataFrame:
        logger.debug(f"read dataframe for partition {partition}")
        input_format = self.detect_format(partition.url)
        # TODO allow for user spec of engine and other params, essentially any quark
        if input_format is InputFormat.PARQUET:
            reader = lambda fd: pd.read_parquet(fd, engine="fastparquet")  # noqa: E731
        elif input_format is InputFormat.JSON:
            reader = lambda fd: pd.read_json(fd, lines=True)  # noqa: E731
        elif input_format is InputFormat.CSV:
            reader = pd.read_csv
        elif input_format is InputFormat.AUTO:
            raise ValueError(f"partition had format detected as auto -> invalid state. Partition: {partition}")
        else:
            assert_exhaustive_enum(input_format)

        try:
            with fs.open(partition.url, "rb") as fd:
                df = reader(fd)
        except FileNotFoundError as e:
            logger.warning(f"file {partition} reading exception {type(e)}, attempting cache invalidation and reread")
            fs.invalidate_cache()
            with fs.open(partition.url, "rb") as fd:
                df = reader(fd)

        for key, value in partition.columns.items():
            df[key] = value
        return df

    def concat(self, data: Iterable[Any]) -> Any:  # TODO generics on the Any
        return pd.concat(data)


PANDAS_READER = PandasReader()


class EnumeratedDictReader(DataReader):
    def read_single(self, partition: Partition, fs: AbstractFileSystem) -> dict:
        logger.debug(f"read single for partition {partition}")
        input_format = self.detect_format(partition.url)
        if input_format != InputFormat.JSON:
            raise ValueError(f"EnumeratedDictReader supports only json, not {input_format}. Partition is {partition}")

        def read_json(url):
            with fs.open(url, "r") as fd:
                return json.load(fd)

        try:
            base = read_json(partition.url)
        except FileNotFoundError as e:
            logger.warning(f"file {partition} reading exception {type(e)}, attempting cache invalidation and reread")
            fs.invalidate_cache()
            base = read_json(partition.url)
        return {**base, **partition.columns}

    def concat(self, data: Iterable[Any]) -> Any:  # TODO generics on the Any
        return dict(enumerate(data))


ENUMERATED_DICT_READER = EnumeratedDictReader()
