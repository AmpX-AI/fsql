import io
import json

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


def test_write_csv_s3(helper):
    """Writes a dataframe as csv, tests that read works."""
    bucket = "test-bouquet"
    fs = helper.s3fs
    fs.mkdir(bucket)

    input = pd.DataFrame({"key": [1]})
    write_object("s3://test-bouquet/my_df.csv", input, format="csv")
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


def test_write_csv_local(tmpdir):
    """Writes a csv file, tests that read works."""
    input = pd.DataFrame({"k1": [1], "k2": [2]})
    path_base = tmpdir.join("my_file.csv")
    url = f"file://{path_base}"
    write_object(url, input, format="csv")
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

    just_bytes = b"hello"
    with pytest.raises(ValueError, match=r"cannot infer writer.*"):
        write_object("file://whatever", just_bytes)


def test_write_vanilla_bytes(tmpdir):
    path = tmpdir.join("f1")
    data = b"ahoj"
    write_object(f"file://{path}", io.BytesIO(data))
    with open(path, "rb") as f:
        extracted = f.read()
    assert extracted == data


def test_write_json_s3(helper):
    """Writes a dataframe as json, tests that read works."""
    bucket = "test-bouquet"
    fs = helper.s3fs
    fs.mkdir(bucket)

    input = pd.DataFrame({"k1": [1, 2], "k2": [3, 4]}, index=["one", "two"])
    write_object("s3://test-bouquet/my_df.json", input, format="json")
    output = helper.read_json_file("s3://test-bouquet/my_df.json")
    expected_output = {"k1": {"one": 1, "two": 2}, "k2": {"one": 3, "two": 4}}
    assert output == expected_output


def test_write_json(tmpdir):
    """Writes a json file, tests that read works."""
    input = pd.DataFrame({"k1": [1, 2], "k2": [3, 4]}, index=["one", "two"])
    path_base = tmpdir.join("my_file.json")
    url = f"file://{path_base}"
    write_object(url, input, format="json")
    with open(path_base, "r") as f:
        output = json.load(f)
    expected_output = {"k1": {"one": 1, "two": 2}, "k2": {"one": 3, "two": 4}}
    assert output == expected_output
