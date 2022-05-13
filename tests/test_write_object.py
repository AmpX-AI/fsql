import csv
import io

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table, write_object
from fsql.query import Q_TRUE


def test_write_buffer_s3(helper):
    """Writes a dataframe as parquet, tests that read works."""
    bucket = "test-bouquet"
    fs = helper.s3fs
    fs.mkdir(bucket)

    input = pd.DataFrame({"key": [1]})
    write_object("s3://test-bouquet/my_df.parquet", input)
    output = read_partitioned_table("s3://test-bouquet/", Q_TRUE)
    assert set(output.key.to_list()) == {1}


def test_write_buffer_local_bytes(tmpdir):
    """Writes a dataframe as parquet, tests that read works."""
    path_base = tmpdir.join("my_file.parquet")
    url = f"file://{path_base}"
    input = pd.DataFrame({"key": [1]})
    write_object(url, input)
    output = pd.read_parquet(path_base)  # for some reason, pd.read_parquet was not happy with file://
    assert set(output.key.to_list()) == {1}


def test_write_buffer_local_string(tmpdir):
    """Writes a csv file, tests that read works."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["k1", "k2"])
    writer.writerow([1, 2])
    path_base = tmpdir.join("my_file.csv")
    url = f"file://{path_base}"
    write_object(url, buffer)
    output = pd.read_csv(path_base)  # for some reason, pd.read_parquet was not happy with file://
    assert set(output.k1.to_list()) == {1}
    assert set(output.k2.to_list()) == {2}


def test_format_engine_option(tmpdir):
    path1 = tmpdir.join("f1.parquet")
    path2 = tmpdir.join("f2.parquet")
    df = pd.DataFrame({"a": [1, 2]})
    write_object(f"file://{path1}", df, "parquet", {"engine": "pyarrow"})
    write_object(f"file://{path2}", df, "parquet", {"engine": "fastparquet"})
    df1 = pd.read_parquet(path1, engine="pyarrow")
    df2 = pd.read_parquet(path2, engine="fastparquet")
    assert_frame_equal(df, df1)
    assert_frame_equal(df, df2)


def test_wrong_formats():
    df = pd.DataFrame({"a": [1, 2]})
    with pytest.raises(ValueError):
        write_object("file://whatever", df, "hdf5")
    with pytest.raises(ValueError):
        write_object("file://whatever", df, "parquet", {"engine": "mercedes"})
    bf = io.StringIO("hello")
    with pytest.raises(ValueError):
        write_object("file://whatever", bf, "parquet")
