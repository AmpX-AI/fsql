import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.column_parser import AutoParser
from fsql.query import Q_AND, Q_EQ, Q_TRUE, AtomicQuery

df1 = pd.DataFrame(data={"c1": [0, 1], "c2": ["hello", "world"]})
df2 = pd.DataFrame(data={"c1": [2, 3], "c2": ["salve", "mundi"]})
df3 = pd.DataFrame(data={"c1": [4, 5], "c2": ["cthulhu", "rlyeh"]})


def test_auto_parser_with_fname(tmp_path):
    """Tests that AutoParser works with filename filtering when parse_filenames_as is passed."""
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
    parser = AutoParser(parse_filenames_as="fname")
    query = Q_AND(Q_EQ("col1", "4"), query)  # this combination lets only df1 through
    case2_result = read_partitioned_table(f"file://{tmp_path}/", query, parser)

    case2_expected = df1.assign(col1="4", col2="5", colX="a", fname="a1.json")
    assert_frame_equal(case2_expected, case2_result)


def test_auto_parser_with_fname_mixed(tmp_path):
    """Tests that AutoParser cannot work with mixed structure."""
    partition1 = tmp_path / "col1=4" / "col2" / "colX=a"
    partition1.mkdir(parents=True)
    df1.to_json(partition1 / "a1.json", orient="records", lines=True)
    partition2 = tmp_path / "col1=4" / "col2" / "colX=b"
    partition2.mkdir(parents=True)
    df2.to_json(partition2 / "a2.json", orient="records", lines=True)
    partition3 = tmp_path / "col1=9" / "col2" / "colX=b"
    partition3.mkdir(parents=True)
    df3.to_json(partition3 / "a1.json", orient="records", lines=True)

    # default auto parser will fail as it expects all partitions in "=" format
    with pytest.raises(ValueError, match="not enough values to unpack"):
        query = Q_AND(Q_EQ("col1", "4"), Q_EQ("fname", "a1.json"))  # this combination lets only df1 through
        read_partitioned_table(f"file://{tmp_path}/", query, AutoParser())

    # with filename parsing, col2 (non-"=" format) is parsed as fname which fails (there is no col2=a1.json)
    with pytest.raises(ValueError, match="No objects to concatenate"):
        query = Q_AND(Q_EQ("col1", "4"), Q_EQ("fname", "a1.json"))  # this combination lets only df1 through
        read_partitioned_table(f"file://{tmp_path}/", query, AutoParser(parse_filenames_as="fname"))

    # similar to the previous one - col2 is parsed as fname and pass the query (fname=col2)
    #  which result in a bit different error (the real filename is "duplication")
    with pytest.raises(ValueError, match="duplicate key inserted: fname"):
        query = Q_AND(Q_EQ("col1", "4"), Q_EQ("fname", "col2"))  # to show confusion that can arise with mixed structure
        read_partitioned_table(f"file://{tmp_path}/", query, AutoParser(parse_filenames_as="fname"))


def test_auto_parser_fname_with_from_str(tmp_path):
    """Tests AutoParser works when combining filtering through 'from_str' method and fname query."""
    partition1 = tmp_path / "col=a"
    partition1.mkdir(parents=True)
    df1.to_json(partition1 / "a1.json", orient="records", lines=True)
    partition2 = tmp_path / "col=b"
    partition2.mkdir(parents=True)
    df2.to_json(partition2 / "a2.json", orient="records", lines=True)
    df3.to_json(partition2 / "a1.json", orient="records", lines=True)

    parser = AutoParser.from_str("col=b", fname="fname")
    case2_result = read_partitioned_table(f"file://{tmp_path}/", Q_EQ("fname", "a1.json"), parser)

    case2_expected = df3.assign(col="b", fname="a1.json")
    assert_frame_equal(case2_expected, case2_result)

    # using fname query directly should give the same result
    parser = AutoParser.from_str("col=b", fname="fname=[a1.json]")
    case2_result = read_partitioned_table(f"file://{tmp_path}/", Q_TRUE, parser)

    case2_expected = df3.assign(col="b", fname="a1.json")
    assert_frame_equal(case2_expected, case2_result)
