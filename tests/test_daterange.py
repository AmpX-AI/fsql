import datetime
import logging

import pandas as pd
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.daterange_utils import DateRangeGenerator, DateRangeQuery
from fsql.query import Q_TRUE

logging.basicConfig(level=logging.DEBUG, force=True)


def test_daterange_utils(tmp_path):
    """In the first example, we show how to read a date range using a Query object.
    Use this when you need to combine the Query with other, more involved conditions.

    The second example is based on DateRangeGenerator, where we instead of listing files
    in the file system, we generate them beforehand. Technically, it is a "column parser",
    though the parsing it does is rather trivial. Use this when you "just need a data
    range", and there are no other partitioning columns than ymd."""

    p1 = tmp_path / "year=2022" / "month=4" / "jaj=30"  # 'jaj' means day in klingon, in case you wonder
    p2 = tmp_path / "year=2022" / "month=5" / "jaj=1"
    p3 = tmp_path / "year=2022" / "month=5" / "jaj=2"
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
    query = DateRangeQuery(start, end, day_name="jaj")
    result_query = read_partitioned_table(f"file://{tmp_path}/", query)

    df1e = df1.assign(year="2022", month="4", jaj="30")
    df2e = df2.assign(year="2022", month="5", jaj="1")
    expect = pd.concat([df1e, df2e])

    assert_frame_equal(result_query, expect)

    parser = DateRangeGenerator.build(start, end, day_name="jaj")
    result_parser = read_partitioned_table(f"file://{tmp_path}/", Q_TRUE, parser)

    assert_frame_equal(result_parser, expect)

    # Note that if you use a long date range, you may end up with a large number of files being merged
    # into a single dataframe. This is one of situations where using Dask may make sense -- feel free to
    # head over to test_dask
