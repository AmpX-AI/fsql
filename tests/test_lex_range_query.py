import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.query import Q_AND, Q_EQ, Q_OR, ColumnComparator, ColumnRange, LexRangeQuery


def test_valid_ranges():
    # just testing that this does not raise is enough
    assert [
        ColumnRange("any", 1, 3, ColumnComparator.num),
        ColumnRange("any", "a", "a"),
    ]


def test_invalid_ranges():
    with pytest.raises(ValueError, match="invalid range"):
        ColumnRange("any", 2, 1, ColumnComparator.num)
    with pytest.raises(ValueError, match="invalid range"):
        ColumnRange("any", "c", "b")


def test_lex_range_query1(tmp_path):
    # this corresponds to c1=[b, d), c2=*, c3=[c, d)
    ranges = [
        ColumnRange("c1", "b", "d"),
        ColumnRange("c2", "", "", ColumnComparator.wld),
        ColumnRange("c3", "c", "d"),
    ]
    # will be omitted due to c3<c
    left_bad1 = tmp_path / "c1=b" / "c2=a" / "c3=a"
    # will be omitted due to c1<b
    left_bad2 = tmp_path / "c1=a" / "c2=a" / "c3=c"
    # will be included
    included1 = tmp_path / "c1=c" / "c2=x" / "c3=d"  # c3 is high, but c1 is low enough to get in
    included2 = tmp_path / "c1=b" / "c2=4" / "c3=c"  # minimum of the interval, accepted
    included3 = tmp_path / "c1=d" / "c2=z" / "c3=c"  # maximum of the interval, accepted
    # will be omitted due to c1>=d and c3>=d -- the smallest excluded case
    right_bad1 = tmp_path / "c1=d" / "c2=w" / "c3=d"
    # will be omitted due to c1>d
    right_bad2 = tmp_path / "c1=e" / "c2=w" / "c3=c"
    for i, p in enumerate((left_bad1, left_bad2, included1, included2, included3, right_bad1, right_bad2)):
        p.mkdir(parents=True)
        df = pd.DataFrame({"k": [i]})
        df.to_csv(p / "f.csv", index=False)

    query = LexRangeQuery(ranges=ranges)
    result_query = read_partitioned_table(f"file://{tmp_path}/", query)
    result_query = result_query.sort_values(by=["k"]).reset_index(drop=True)

    expect = pd.DataFrame(
        {"k": [2, 3, 4], "c1": ["c", "b", "d"], "c2": ["x", "4", "z"], "c3": ["d", "c", "c"]}
    ).reset_index(drop=True)

    assert_frame_equal(result_query, expect)


def test_lex_range_query2(tmp_path):
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

    query = LexRangeQuery(ranges=ranges)
    result_query = read_partitioned_table(f"file://{tmp_path}/", query)
    result_query = result_query.sort_values(by=["k"]).reset_index(drop=True)

    expect = pd.DataFrame({"k": [1, 2, 3, 4], "c1": ["3", "9", "11", "21"]}).reset_index(drop=True)

    assert_frame_equal(result_query, expect)


def test_combination(tmp_path):
    range_q = LexRangeQuery(ranges=[ColumnRange("c1", "1", "5", ColumnComparator.num)])
    equal_q = Q_EQ("c1", "7")
    or_q = Q_OR(equal_q, range_q)
    and_q = Q_AND(equal_q, range_q)

    excluded1 = tmp_path / "c1=0"
    included1 = tmp_path / "c1=1"
    included2 = tmp_path / "c1=3"
    excluded2 = tmp_path / "c1=6"
    included3 = tmp_path / "c1=7"
    excluded3 = tmp_path / "c1=8"

    for i, p in enumerate((excluded1, included1, included2, excluded2, included3, excluded3)):
        p.mkdir(parents=True)
        df = pd.DataFrame({"k": [i]})
        df.to_csv(p / "f.csv", index=False)

    result_query_or = read_partitioned_table(f"file://{tmp_path}/", or_q)
    result_query_or = result_query_or.sort_values(by=["k"]).reset_index(drop=True)

    expect_or = pd.DataFrame({"k": [1, 2, 4], "c1": ["1", "3", "7"]}).reset_index(drop=True)

    assert_frame_equal(result_query_or, expect_or)

    with pytest.raises(ValueError, match="No objects to concatenate"):  # TODO empty df would be nicer
        read_partitioned_table(f"file://{tmp_path}/", and_q)
