from __future__ import annotations

import itertools
import logging
from collections.abc import Iterable
from typing import Any, Iterator

import dask
import dask.dataframe as dd
from fsspec.spec import AbstractFileSystem

from fsql.deser import DataReader, PandasReader, PartitionReadFailure, PartitionReadOutcome
from fsql.partition import Partition

logger = logging.getLogger(__name__)


class DaskReader(DataReader):
    """Works by reading every single partition via PandasReader in a delayed task, concatenating together,
    and returning the resulting Dask DataFrame.

    There is one inconvenience -- in order to create the Dask DataFrame, we need the `meta` (ie, the schema).
    If the user does not provide it, we *compute* the first partition here, derive `meta` from it, and proceed
    by turning the remaining Delayed tasks into the Dask DataFrame.
    """

    def __init__(self, meta=None, pandas_reader=None, lazy_errors=False):
        super().__init__(lazy_errors=lazy_errors)
        if pandas_reader and pandas_reader.lazy_errors:
            logger.warning("provided pandas reader has lazy_errors=True, which makes no sense as it is delayed")
        self.pandas_reader = pandas_reader if pandas_reader else PandasReader(lazy_errors=False)
        self.meta = meta

    def read_single(self, partition: Partition, fs: AbstractFileSystem) -> PartitionReadOutcome:
        try:
            # PandasReader returns PartitionReadOutcome, not DataFrame -- so we need to convert accordingly
            convert = lambda pandas_outcome: pandas_outcome[0][0]  # noqa: E731
            delayed_reader = lambda partition, fs: convert(self.pandas_reader.read_single(partition, fs))  # noqa: E731
            return [dask.delayed(delayed_reader)(partition, fs)], []
        except ValueError as e:
            if not self.lazy_errors:
                raise
            else:
                return [], [PartitionReadFailure(partition, str(e))]

    def concat(self, data: Iterable[Any]) -> Any:  # TODO generics on the Any... which is tricky as dd aint Delayed!
        if self.meta:
            dd_iter: Iterator[Any] = (dd.from_delayed(e, meta=self.meta) for e in data)
        else:
            iterator = iter(data)
            head = next(iterator).compute()
            tail = (dd.from_delayed(e, meta=head) for e in iterator)
            head_dd = dd.from_pandas(head, npartitions=1)
            dd_iter = itertools.chain([head_dd], tail)
        # looks like a nuissance of dask, they require list instead of any Iterable...
        return dd.concat(list(dd_iter))
