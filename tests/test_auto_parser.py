import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.column_parser import AutoParser
from fsql.query import Q_AND, Q_EQ, AtomicQuery

df1 = pd.DataFrame(data={"c1": [0, 1], "c2": ["hello", "world"]})
df2 = pd.DataFrame(data={"c1": [2, 3], "c2": ["salve", "mundi"]})
df3 = pd.DataFrame(data={"c1": [4, 5], "c2": ["cthulhu", "rlyeh"]})


def test_auto_parser_with_fname(tmp_path):
    partition1 = tmp_path / "col1=4" / "col2=5" / "colX=a"
    partition1.mkdir(parents=True)
    df1.to_json(partition1 / "a1.json", orient="records", lines=True)
    partition2 = tmp_path / "col1=4" / "col2=6" / "colX=b"
    partition2.mkdir(parents=True)
    df2.to_json(partition2 / "a2.json", orient="records", lines=True)
    partition3 = tmp_path / "col1=9" / "col2=6" / "colX=b"
    partition3.mkdir(parents=True)
    df3.to_json(partition3 / "a1.json", orient="records", lines=True)

    # could be replaced with Q_EQ("fname", "a1.json")
    def query_func(fname: str) -> bool:
        # matches df1 and df3
        return fname.startswith("a1")

    query = AtomicQuery(query_func)
    parser = AutoParser(parse_filenames=True)
    query = Q_AND(Q_EQ("col1", "4"), query)  # this combination lets only df1 through
    case2_result = read_partitioned_table(f"file://{tmp_path}/", query, parser)

    case2_expected = df1.assign(col1="4", col2="5", colX="a", fname="a1.json")
    assert_frame_equal(case2_expected, case2_result)


def test_auto_parser_with_fname_broken(tmp_path):
    partition1 = tmp_path / "col1=4" / "col2" / "colX=a"
    partition1.mkdir(parents=True)
    df1.to_json(partition1 / "a1.json", orient="records", lines=True)
    partition2 = tmp_path / "col1=4" / "col2" / "colX=b"
    partition2.mkdir(parents=True)
    df2.to_json(partition2 / "a2.json", orient="records", lines=True)
    partition3 = tmp_path / "col1=9" / "col2" / "colX=b"
    partition3.mkdir(parents=True)
    df3.to_json(partition3 / "a1.json", orient="records", lines=True)

    # could be replaced with Q_EQ("fname", "a1.json")
    def query_func(fname: str) -> bool:
        # matches df1 and df3
        return fname.startswith("a1")

    query = AtomicQuery(query_func)
    parser = AutoParser(parse_filenames=True)
    query = Q_AND(Q_EQ("col1", "4"), query)  # this combination lets only df1 through

    # TODO: although the query should match df1 it does not as the partitioning is broken
    with pytest.raises(ValueError, match="No objects to concatenate"):
        read_partitioned_table(f"file://{tmp_path}/", query, parser)
