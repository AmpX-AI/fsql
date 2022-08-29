# fsql

The `fsql` package's goal is to simplify the task of getting data from any file system, local or remote, possibly divided into multiple partitions, into a single data frame.
The package has querying capabilities, thus the name stands for "file system query language".

## Quick Start
The core is installed just via `pip install fsql`. Additional filesystem support or output representation is installed via `pip install fsql[s3]` or `pip install fsql[dask]`.

For examples of usage (and sort-of documentation), we use selected test files accompanied with explanatory comments:
1. [basic usage](tests/test_example_usage.py),
2. [date range utils](tests/test_daterange.py),
3. [integrating with Dask](tests/test_dask.py).

## Use Cases & Features
The canonical usecase is that you have data on `S3` stored e.g. as `<table_name>/year=<yyyy>/month=<mm>/day=<dd>/<filename>.csv`, and you want to fetch a part of it (e.g., a week from the last month, every Monday last year, ...) as a single Pandas or Dask DataFrame, via a short command -- without having to write the `boto3` crawl, the bytes2csv, the csv2pandas, etc.
The crawl/query part is traditionally covered by metastores, such as Hive Metastore or Glue Data Catalog.

Why, then, would you use `fsql`?
* If you don't have the metastore set up, using `fsql` is faster and has no operation/maintenance costs.
* If your processing engine (Pandas, Dask, ...) does not cooperate with the metastore out of the box, plugging it to `fsql` is simpler.
* If the storage structure is not supported by the metastore -- e.g., usually the `<columnName>=<value>` is required, yet we often encounter just `<value>` with the column name provided externally.
* If the name of the file bears any information, e.g., it is the timestamp of the event the file represent, or some id of the data source -- metastores usually treat individual filenames as meaningless (for a good reason, as distributed engines such as Spark just use hashes), however, we treat them as regular column.
* If the condition for fetching data is not trivial (not available as SQL function -- e.g., you want to fetch only those files for which some column returns 1 when fed to an ML model), the implementation provided by `fsql` is more efficient than manually fetching all partitions from metastore and evaluating locally.
* You query from multiple filesystems at once (`S3` and `GDrive`), and you don't have a unifying layer -- `fsql` changes between those just by changing the URL prefix. This can be particularly handy in some integration tests, if don't want to query real S3 but keep the same code (and point it at local filesystem or Minio instead... we find it less hassle and more value than with mocking, which fits more to unit tests).

However, if you have your metastore and are happy with it, there is no reason not to use it.
There are some advantages which `fsql` will likely never cover:
* A metastore may allow for data discovery based on additional metadata -- but `fsql` is not backed by any persistence to hold such data.
* There is no difference between a partition column and regular column inside the data, you conveniently query both the same way and may combine them -- however, we believe that due to severe performance difference, those should be kept separated until the data is fetched to the user's computing process.

There is also some overlap with `pandas.io.sql` -- that one, however, focuses solely on pandas whereas `fsql` can adapt to any data processing tool which allows partition-based input specification (e.g., Spark).
On the other hand, `pandas.io.sql` has good integration with `sql-alchemy` and traditional database queries, whereas `fsql` is focused on partitioned file systems only.

## Supported & Underlying Technologies
This package is based on `fsspec` -- anything supported by that can be plugged in.
At the moment, we have test coverage only for local filesystem and `s3`.
Adding a new one requires mostly ensuring that authentication and URL parsing will work correctly, and taking care of some weird cornercases such as caching in `s3fs`.

The supported output representations are at the moment `Pandas`, `Dask` and `list[dict]`.
Adding a new one requires implementing a conversion from a `Iterable[(Path, FileSystem)]` to the desired object.

The query language is rather simplistic, so no proper parser & grammar & query optimiser is used at the moment.

## License
[BSD 3](LICENSE)
