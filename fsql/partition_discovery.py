"""Controls the main logic of iterative crawling and filter invocation.

This is the module that ties `query` and `column_parser` together. It is, at the moment,
least-well designed and most fragile. One has to be careful when extending this, to not
cause severe malperformance.

This module is not principally part of the public API, we don't expect any behaviour here
to be customisable.
"""

# TODO improve design, upgrade the doc

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import chain, groupby

from fsspec.spec import AbstractFileSystem

from fsql.column_parser import ColumnParser
from fsql.partition import Partition
from fsql.query import Query

logger = logging.getLogger(__name__)
flat_map = lambda f, xs: (y for ys in xs for y in f(ys))  # noqa: E731


@dataclass
class DirectoryListing:
    files: Iterable[str]
    directories: Iterable[str]


def list_directory(url: str, fs: AbstractFileSystem) -> DirectoryListing:
    listing_raw = fs.ls(url, detail=True)
    logger.debug(f"url {url} listed to {len(listing_raw)} elements")
    selector = lambda e: e["type"]  # noqa: E731
    extractor = lambda l: sorted(list(map(lambda e: e["name"].split("/")[-1], l)))  # noqa: E731
    # we need the extractor to materialise to list, because the groupby iterator is shared
    listing_grouped = {k: extractor(v) for k, v in groupby(sorted(listing_raw, key=selector), selector)}
    listing = DirectoryListing(files=listing_grouped.get("file", []), directories=listing_grouped.get("directory", []))
    logger.debug(f"url {url} listed to {listing}")
    return listing


def discover_partitions(
    query: Query, column_parser: ColumnParser, partition: Partition, fs: AbstractFileSystem
) -> Iterable[Partition]:
    # NOTE expose in the query the option to look at last item only, do the expand by as a flat map

    # NOTE this whole thing is quite hacky. Before querying fs, we check whether the user has prescribed the
    # values for columns at this stage exactly. The weird thing is that we still parse and query those columns later,
    # but that may actually be upside. Also, the separation between files and directories does feel a bit artificial
    # here
    logger.debug(f"partition discovery with query {query} and partition {partition}")
    if not partition.url.endswith("/"):
        partition.url += "/"  # required due to fsspec.ls not appending '/' to listed directories
    generated_partitions = column_parser.generate()
    if generated_partitions:
        if column_parser.is_terminal_level():
            listing = DirectoryListing(files=generated_partitions, directories=[])
        else:
            listing = DirectoryListing(files=[], directories=[partition + "/" for partition in generated_partitions])
    else:
        listing = list_directory(partition.url, fs)

    subdir_partitions = (partition.expand_by(item, column_parser(item)) for item in listing.directories)
    subdir_partitions_flt = filter(lambda partition: query.eval_available(partition.columns), subdir_partitions)
    # NOTE paralellisation opportunity! We don't need this to be sequential
    subdir_partitions_exp = flat_map(
        lambda partition: discover_partitions(query, column_parser.tail(partition), partition, fs),
        subdir_partitions_flt,
    )

    file_partitions_flt: Iterable[Partition] = iter(())
    if column_parser.is_terminal_level():
        if column_parser.parses_filenames():
            # TODO allow filename filtering in auto column extraction as well...
            # TODO distinguish for the fixed parser a column for the fnames...
            file_partitions = (partition.expand_by(item, column_parser(item)) for item in listing.files)
            file_partitions_flt = filter(lambda partition: query.eval_all(partition.columns), file_partitions)
        else:
            if not query.eval_all(partition.columns):
                file_partitions_flt = []
            else:
                file_partitions_flt = (partition.expand_by(item, None) for item in listing.files)

    return chain(subdir_partitions_exp, file_partitions_flt)
