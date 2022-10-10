"""Provides a layer that presents file system in a table-like fashion, to simplify bulk loading into data frames.

The goal is not to replace advanced tools such as hive metastore, athena/wrangler, spark.
Rather, we aim to cover some corner cases where those tools are not set up or cannot be applied.
In the case of tables being stored with:
 * `<key>=<value>` format of partition columns,
 * parquet files having flat schema,
 * filenames bearing no information,
this module should bring no advantage over general tools.

Primary API consists of:
 - `read_partitioned_table`,
 - `write_object`.

The conceptual separation of submodules consists of:
 - `column_parser` captures how the filesystem path is translated into table *partition* columns,
 - `query` handles filtering of the partition columns,
 - `deser` deals with conversion of the raw bytes into various dataframes or other user-prefered structures,
 - `partition_discovery` -- controls the main logic of iterative crawling and filter invocation.

At the moment, interactions with an underlying filesystem is handled via the `fsspec` lib -- which is externally
exposed so that the user can fully configure it at will.

A listing of a filesystem, if done without care, can result in a needless delays and extra costs.
Some care is paid to optimisation here, mostly in the `partition_discovery` module. In brief, the user is
advised to:
1. use `column_parser`'s generative functionality, if a range of some column is small -- this is invoked *before*
   the call to list file system's content,
2. use `query`'s composition of queries, as opposed to compose on their own, and use as few columns as possible
   in the individual subqueries (atomic statements) -- this is because subqueries are evaluated eagerly, once all
   columns are available.

As with every distributed query system, one should understand there are no transactional guarantees. In particular:
 - if, between partition discovery and reading, a file is deleted, the query crashes with FileNotFound
 - if, between partition discovery and reading, a file is added, the query does not process it.
"""

from __future__ import annotations

import io
import logging
import shutil
import warnings
from typing import Optional, Union

import pandas as pd
from fsspec.spec import AbstractFileSystem

from fsql import get_url_and_fs
from fsql.column_parser import AUTO_PARSER, ColumnParser
from fsql.deser import PANDAS_READER, DataObject, DataObjectRich, DataReader
from fsql.partition import Partition
from fsql.partition_discovery import discover_partitions
from fsql.query import Query

logger = logging.getLogger(__name__)


def read_s3_table(
    url: str,
    query: Query,
    column_parser: ColumnParser = AUTO_PARSER,
    data_reader: DataReader = PANDAS_READER,
):
    """Old deprecated name, use `read_partitioned_table` instead"""
    warnings.warn("Function `read_s3_table` is deprecated, use `read_partitioned_table` instead", DeprecationWarning)
    return read_partitioned_table(url, query, column_parser, data_reader)


def read_partitioned_table(
    url: str,
    query: Query,
    column_parser: ColumnParser = AUTO_PARSER,
    data_reader: DataReader[DataObject] = PANDAS_READER,  # type: ignore
    fs: Optional[AbstractFileSystem] = None,
) -> Union[DataObject, DataObjectRich]:
    """Reads a table rooted at `url`, with partition columns described in `column_parser` and filtered via `query`.

    The default values assume `colName1=val/colName2=val` format of the path, and pandas data frame as output format.
    There is no default query -- for reading all partitions, user is supposed to use the always-true query from the
    `query` module.

    Note that the provided DataReaders launch a ThreadPoolExecutor when downloading individual files, to speed up I/O.

    If `fs` is not provided, a default one is constructed from the url. The instance is then used for all `ls`
    and `open` operations.

    Return type is driven by the `data_reader` -- default behaviour is to return a pandas DataFrame. All provided
    data readers raise exception whenever any Partition cannot be read, and support `lazy_errors` option which changes
    the behaviour to collect all exceptions instead and return them together with an object consisting of all that was
    readable.
    """
    url_suff, fs_default = get_url_and_fs(url)
    fs = fs if fs else fs_default
    # we are relying on the invariant that directory urls end with '/'
    # we thus need to check whether the user is not fooling us
    if not url_suff.endswith("/"):
        if fs.isdir(url_suff):
            url_suff += "/"

    root_partition = Partition(url_suff, {})
    logging.debug(f"partition discovery starting. Url: {url_suff}, Query: {query}")
    partitions = discover_partitions(query, column_parser, root_partition, fs)
    partitions = list(partitions)
    logging.debug(f"partitions are {partitions}")
    logging.debug(f"data fetch starting. Url: {url}, Query: {query}")
    return data_reader.read_and_concat(partitions, fs)


def write_object(
    url: str,
    data: Union[pd.DataFrame, io.StringIO, io.BytesIO],
    format: Optional[str] = None,
    format_options: Optional[dict[str, str]] = None,
    fs: Optional[AbstractFileSystem] = None,
) -> None:
    """Minimalistic function to write an object to a designated location.

    * Does not support any table-like semantics -- partition appends or multi-partition inserts,
    * for `io` objects, `seek(0)` and `shutil` write is the behaviour; `format` argument must not be specified,
    * for Data Frames converts to the specified format (only parquet+fastparquet for now), and then it is written
      using the file-like object from fsspec and dataframe's native write.

    At the moment, only parquet and csv as a format are supported. The only format options are 'engine' with values
    'fastparquet' and 'pyarrow' (these refer only to parquet format).

    For larger data frames or table-like semantics, use rather endpoint (TODO).

    If `fs` is not provided, a default one is constructed from the url. The instance is then used for an `open`
    operation.
    """

    url_suff, fs_default = get_url_and_fs(url)
    fs = fs if fs else fs_default
    format_options = format_options if format_options else {}

    if isinstance(data, pd.DataFrame):
        if format == "parquet" or format is None:
            engine = format_options.get("engine", "fastparquet")
            if engine == "fastparquet":
                data.to_parquet(url_suff, engine=engine, open_with=fs.open)
            elif engine == "pyarrow":
                with fs.open(url_suff, "wb") as fd:
                    data.to_parquet(fd, engine=engine)
            else:
                raise ValueError(f"unsupported engine for dataframe writing: {engine}")
        elif format == "csv":
            with fs.open(url_suff, "wb") as fd:
                data.to_csv(fd)
        else:
            raise ValueError(f"unsupported format for dataframe writing: {format}")
    elif isinstance(data, io.StringIO) or isinstance(data, io.BytesIO):
        if format:
            raise ValueError(f"cannot specify format when data is a buffer. Provided format: {format}")
        data.seek(0)
        mode = "wb" if isinstance(data, io.BytesIO) else "w"
        with fs.open(url_suff, mode) as fd:
            shutil.copyfileobj(data, fd)
    else:
        raise ValueError(f"cannot infer writer for object of type {type(data)}")
