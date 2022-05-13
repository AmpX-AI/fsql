import datetime
import logging

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.daterange_utils import DateRangeGenerator, DateRangeQuery
from fsql.query import Q_AND, Q_EQ, Q_TRUE

logging.basicConfig(level=logging.DEBUG, force=True)


def test_daterange_combined_query(tmp_path):
    """Test combination of date range query and other query"""
    p1 = tmp_path / "col=1" / "year=2022" / "month=4" / "day=30"
    p2 = tmp_path / "col=1" / "year=2022" / "month=5" / "day=1"
    p3 = tmp_path / "col=2" / "year=2022" / "month=5" / "day=2"
    p1.mkdir(parents=True)
    p2.mkdir(parents=True)
    p3.mkdir(parents=True)
    df1 = pd.DataFrame({"k": [1]})
    df2 = pd.DataFrame({"k": [2]})
    df3 = pd.DataFrame({"k": [3]})
    df1.to_csv(p1 / "f1.csv", index=False)
    df2.to_csv(p2 / "f2.csv", index=False)
    df3.to_csv(p3 / "f3.csv", index=False)

    start = "2022/4/30"
    end = datetime.date(2022, 5, 3)
    queryL = Q_EQ("col", "1")
    queryR = DateRangeQuery(start, end)
    result_query = read_partitioned_table(f"file://{tmp_path}/", Q_AND(queryL, queryR))

    df1e = df1.assign(col="1", year="2022", month="4", day="30")
    df2e = df2.assign(col="1", year="2022", month="5", day="1")
    expect = pd.concat([df1e, df2e])

    assert_frame_equal(result_query, expect)


def test_daterange_fixed_fmt(tmp_path):
    """Test date range generator without column names"""
    p1 = tmp_path / "2022" / "4" / "30"
    p2 = tmp_path / "2022" / "5" / "1"
    p3 = tmp_path / "2022" / "5" / "2"
    p1.mkdir(parents=True)
    p2.mkdir(parents=True)
    p3.mkdir(parents=True)
    df1 = pd.DataFrame({"k": [1]})
    df2 = pd.DataFrame({"k": [2]})
    df3 = pd.DataFrame({"k": [3]})
    df1.to_csv(p1 / "f1.csv", index=False)
    df2.to_csv(p2 / "f2.csv", index=False)
    df3.to_csv(p3 / "f3.csv", index=False)

    start = "2022/4/30"
    end = datetime.date(2022, 5, 2)
    parser = DateRangeGenerator.build(start, end, year_name="annus", include_column_in_path=False)
    result_query = read_partitioned_table(f"file://{tmp_path}/", Q_TRUE, parser)

    df1e = df1.assign(annus="2022", month="4", day="30")
    df2e = df2.assign(annus="2022", month="5", day="1")
    expect = pd.concat([df1e, df2e])

    assert_frame_equal(result_query, expect)


def test_fail_internally_broken(tmp_path):
    """This test is kinda weird -- we try to simulate the condition where the generator is forced to
    parse beyond just ymd partitions. This can never happen in the code due to is_terminal_level
    protection, so we need to simulate with a hack"""
    p1 = tmp_path / "year=2022" / "month=4" / "day=30" / "extra=column"
    p1.mkdir(parents=True)
    df1 = pd.DataFrame({"k": [1]})
    df1.to_csv(p1 / "f1.csv", index=False)

    start = "2022/4/30"
    end = datetime.date(2022, 5, 1)
    parser = DateRangeGenerator.build(start, end)
    parser.is_terminal_level = lambda: False
    with pytest.raises(ValueError, match="unexpected call of tail -- internal failure to terminate discovery"):
        read_partitioned_table(f"file://{tmp_path}/", Q_TRUE, parser)
