import pandas as pd
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.query import ColumnComparator, ColumnRange, SimpleRangeQuery


def test_simple_range_query1(tmp_path):
    # this corresponds to c1=[b, d), c2=*, c3=[c, e)
    ranges = [
        ColumnRange("c1", "b", "d"),
        ColumnRange("c2", "", "", ColumnComparator.wld),
        ColumnRange("c3", "c", "d"),
    ]
    # will be omitted due to c3<b
    left_bad1 = tmp_path / "c1=b" / "c2=a" / "c3=a"
    # will be omitted due to c1<b
    left_bad2 = tmp_path / "c1=a" / "c2=a" / "c3=c"
    # will be included
    included1 = tmp_path / "c1=c" / "c2=x" / "c3=d"
    included2 = tmp_path / "c1=b" / "c2=4" / "c3=c"
    # will be omitted due to c3>=e
    right_bad1 = tmp_path / "c1=b" / "c2=w" / "c3=e"
    # will be omitted due to c1>=d
    right_bad2 = tmp_path / "c1=d" / "c2=w" / "c3=c"
    for i, p in enumerate((left_bad1, left_bad2, included1, included2, right_bad1, right_bad2)):
        p.mkdir(parents=True)
        df = pd.DataFrame({"k": [i]})
        df.to_csv(p / "f.csv", index=False)

    query = SimpleRangeQuery(ranges=ranges)
    result_query = read_partitioned_table(f"file://{tmp_path}/", query)
    result_query = result_query.sort_values(by=["k"]).reset_index(drop=True)

    expect = pd.DataFrame({"k": [2, 3], "c1": ["c", "b"], "c2": ["x", "4"], "c3": ["d", "c"]}).reset_index(drop=True)

    assert_frame_equal(result_query, expect)


def test_simple_range_query2(tmp_path):
    # this corresponds to c1=[3, 27)
    ranges = [
        ColumnRange("c1", "3", "27", ColumnComparator.num),
    ]
    # will be omitted due to c1<3
    left_bad1 = tmp_path / "c1=1"
    # will be included
    included1 = tmp_path / "c1=3"
    included2 = tmp_path / "c1=9"
    included3 = tmp_path / "c1=11"
    included4 = tmp_path / "c1=21"
    # will be omitted due to c1>=27
    right_bad1 = tmp_path / "c1=27"
    right_bad2 = tmp_path / "c1=101"
    for i, p in enumerate((left_bad1, included1, included2, included3, included4, right_bad1, right_bad2)):
        p.mkdir(parents=True)
        df = pd.DataFrame({"k": [i]})
        df.to_csv(p / "f.csv", index=False)

    query = SimpleRangeQuery(ranges=ranges)
    result_query = read_partitioned_table(f"file://{tmp_path}/", query)
    result_query = result_query.sort_values(by=["k"]).reset_index(drop=True)

    expect = pd.DataFrame({"k": [1, 2, 3, 4], "c1": ["3", "9", "11", "21"]}).reset_index(drop=True)

    assert_frame_equal(result_query, expect)
