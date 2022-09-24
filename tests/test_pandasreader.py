import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.deser import InputFormat, PandasReader
from fsql.query import Q_TRUE

df1 = pd.DataFrame(data={"c1": [0, 1], "c2": ["hello", "world"]})
df2 = pd.DataFrame(data={"c1": [2, 3], "c2": ["hello", "world"], "c3": [0.1, 0.2]})


def test_input_format_override(tmp_path):
    """Test that explicitly setting format overrides suffix."""

    case1_path = tmp_path / "table1"
    case1_path.mkdir(parents=True)
    df1.to_csv(case1_path / "f1.json", index=False)  # confuse the default by bad suffix

    with pytest.raises(ValueError, match="Expected object or value"):
        # this test condition is quite brittle! A better match would be desired
        read_partitioned_table(f"file://{case1_path}/", Q_TRUE)

    reader = PandasReader(input_format=InputFormat.CSV)
    succ_result = read_partitioned_table(f"file://{case1_path}/", Q_TRUE, data_reader=reader)
    assert_frame_equal(df1, succ_result)


def test_parquet_kwargs(tmp_path):
    """Test that a kwarg (`columns`) gets passed through and obeyed."""

    case1_path = tmp_path / "table1"
    case1_path.mkdir(parents=True)
    df1.to_parquet(case1_path / "f1.parquet", index=False)

    reader = PandasReader(columns=["c2"])
    result = read_partitioned_table(f"file://{case1_path}/", Q_TRUE, data_reader=reader)
    assert_frame_equal(df1[["c2"]], result)


def test_lazy_errors(tmp_path):
    """Create two parquets with different schema and test that only one is correctly read."""
    case1_path = tmp_path / "table1"
    case1_path.mkdir(parents=True)
    df1.to_parquet(case1_path / "f1.parquet", index=False)
    df2.to_parquet(case1_path / "f2.parquet", index=False)

    error_line = "Following columns were requested but are not available: {'c3'}."
    with pytest.raises(ValueError, match=error_line):
        reader_eager = PandasReader(columns=["c3"])
        result = read_partitioned_table(f"file://{case1_path}/", Q_TRUE, data_reader=reader_eager)
    reader_lazy = PandasReader(columns=["c3"], lazy_errors=True)
    result = read_partitioned_table(f"file://{case1_path}/", Q_TRUE, data_reader=reader_lazy)
    assert_frame_equal(df2[["c3"]], result.data)
    reasons = [e.reason.split("\n")[0] for e in result.failures]
    assert reasons == [error_line]
