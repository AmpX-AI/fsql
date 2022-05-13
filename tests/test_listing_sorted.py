import os

import fsspec
import pandas as pd
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table, write_object
from fsql.partition_discovery import list_directory
from fsql.query import Q_TRUE


def test_fs_sorted(tmpdir, helper):
    """Validates that files on a filesystem are listed in sorted order.

    Ideally, this would iterate over all FS implementation so that a newly added one conforms as well.
    This test does both the unit test part of the `list_directory` and the integration part of the
    `read_partitioned_table` (which includes the `concat`)"""

    df1 = pd.DataFrame({"a": [1, 2, 3]})
    df2 = pd.DataFrame({"b": [3, 4, 5]})
    df3 = pd.DataFrame({"c": [6, 7, 8]})
    dfs = [df1, df2, df3]
    df_expected = pd.concat(dfs)
    filenames = ["2022_1_1.csv", "2022_1_2.csv", "2022_1_3.csv"]
    for df, fname in zip(dfs, filenames):
        df.to_csv(os.path.join(tmpdir, fname), index=False)
    fs_loc = fsspec.filesystem("file")
    assert list_directory(tmpdir, fs_loc).files == filenames
    df = read_partitioned_table(f"file://{tmpdir}/", Q_TRUE)
    assert_frame_equal(df, df_expected)

    bucket = "test-bouquet"
    fs_s3 = helper.s3fs
    fs_s3.invalidate_cache()  # TODO this is unfortunate -- there is some state sharing across tests. Fix properly
    fs_s3.mkdir(bucket)
    for df, fname in zip(dfs, filenames):
        # TODO this is a bit clunky -- once we support csv output format, should be removed
        write_object(f"s3://{bucket}/{fname.replace('csv', 'parquet')}", df)

    listing = [f.replace("parquet", "csv") for f in list_directory(f"/{bucket}/", fs_s3).files]
    assert listing == filenames
    df = read_partitioned_table(f"s3://{bucket}/", Q_TRUE)
    assert_frame_equal(df, df_expected)
