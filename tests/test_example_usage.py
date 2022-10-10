import hashlib

import pandas as pd
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.column_parser import AutoParser, FixedColumnsParser
from fsql.query import Q_AND, Q_EQ, Q_TRUE, AtomicQuery

df1 = pd.DataFrame(data={"c1": [0, 1], "c2": ["hello", "world"]})
df2 = pd.DataFrame(data={"c1": [2, 3], "c2": ["salve", "mundi"]})
df3 = pd.DataFrame(data={"c1": [4, 5], "c2": ["cthulhu", "rlyeh"]})


def test_single_file(tmp_path):
    """In the first example, we show how to read a single file to a DataFrame.
    We will not use any query, but we show how to turn parts of the path into columns."""

    # the very first case is just "plain read file" from a local filesystem
    case1_path = tmp_path / "table1"
    case1_path.mkdir(parents=True)
    df1.to_csv(case1_path / "f1.csv", index=False)

    # **this** is the fsql read command:
    case1_result = read_partitioned_table(f"file://{case1_path}/", Q_TRUE)  # Q_TRUE means "yes read everything"

    assert_frame_equal(df1, case1_result)

    # for the second case, we use the =-format, ie /<columnName>=<value>/, on the path
    case2_path = tmp_path / "table2"
    case2_data = case2_path / "c3=42" / "c4=test"
    case2_data.mkdir(parents=True)
    df1.to_csv(case2_data / "f1.csv", index=False)

    # the default behaviour is to infer the column names and values, assuming the =-format
    case2_result = read_partitioned_table(f"file://{case2_path}/", Q_TRUE)

    case2_expected = df1.assign(c3="42", c4="test")  # path is automatically columns in dataframe!
    assert_frame_equal(case2_expected, case2_result)  # note however that c3 is a string -- we dont guess dtype!

    # for the third and final case here, we show the same but without the column names on the path
    case3_path = tmp_path / "table3"
    case3_data = case3_path / "42" / "test"
    case3_data.mkdir(parents=True)
    df1.to_csv(case3_data / "f1.csv", index=False)

    # now we need to fix the column names explicitly
    parser = FixedColumnsParser.from_str("a1/a2/fname")
    case3_result = read_partitioned_table(f"file://{case3_path}/", Q_TRUE, parser)

    case3_expected = df1.assign(a1="42", a2="test", fname="f1.csv")
    # note how the fname got included -- this is the default behaviour of FixedColumnsParser
    assert_frame_equal(case3_expected, case3_result)

    # We did not have to specify that the target files are csv. This is infered from the suffix.
    # You can even mix different formats in a single directory, if you think that wise.
    # If you need to define that explicitly, you can supply your own instance of DataReader to read_partitioned_table


def test_multiple_files(tmp_path):
    """In the second example, we show how to read data from multiple partitions into a single DataFrame.
    We show two ways how to do that -- explicit list, and a query."""

    partition1 = tmp_path / "col1=4" / "col2=5" / "colX=a"
    partition1.mkdir(parents=True)
    df1.to_json(partition1 / "f1.json", orient="records", lines=True)
    partition2 = tmp_path / "col1=4" / "col2=6" / "colX=b"
    partition2.mkdir(parents=True)
    df2.to_json(partition2 / "f2.json", orient="records", lines=True)
    partition3 = tmp_path / "col1=9" / "col2=6" / "colX=b"
    partition3.mkdir(parents=True)
    df3.to_json(partition3 / "f3.json", orient="records", lines=True)

    # AutoParser is what we use to call the =-format. Here, we add desired values for the columns
    # We specify a single value for col1, a list for col2, and colX can be anything
    parser = AutoParser.from_str("col1=4/col2=[5,6]/colX")
    case1a_result = read_partitioned_table(f"file://{tmp_path}", Q_TRUE, parser)

    # we omitted the col1=9, thus the df3/partition3 is not expected
    case1a_expected = pd.concat(
        [
            df1.assign(col1="4", col2="5", colX="a"),
            df2.assign(col1="4", col2="6", colX="b"),
        ]
    )
    assert_frame_equal(case1a_expected, case1a_result)

    # if you don't specify a value of a column, it is taken as `*`
    parser = AutoParser.from_str("col1/col2=[6]/colX")
    case1b_result = read_partitioned_table(f"file://{tmp_path}", Q_TRUE, parser)
    # now we don't expect df1 instead: col1=4/col2=5 -- the col1 condition is ok, but col2 is not
    case1b_expected = pd.concat(
        [
            df2.assign(col1="4", col2="6", colX="b"),
            df3.assign(col1="9", col2="6", colX="b"),
        ]
    )
    assert_frame_equal(case1b_expected, case1b_result)

    # For the second case, we use a real Query -- that means a function (colNames*) -> bool
    # We can specify only a subset of columns -- and we should, so that the query can be evaluated early
    def weird_query_func(col2: str, colX: str) -> bool:
        # believe it or not, this matches df2 and df3's partitions
        return hashlib.md5((col2 + colX).encode("ascii")).hexdigest()[0] == "d"

    weird_query = AtomicQuery(weird_query_func)
    query = Q_AND(Q_EQ("col1", "9"), weird_query)  # this combination lets only df3 through
    case2_result = read_partitioned_table(f"file://{tmp_path}/", query)

    case2_expected = df3.assign(col1="9", col2="6", colX="b")
    assert_frame_equal(case2_expected, case2_result)

    # You can mix up queries and explicit lists of partition values in parser. In general, prefer explicit lists,
    # because that saves you `ls` calls to the filesystem (which may be expensive in cloud file systems). Also,
    # we support eager evaluation and circuit breaking -- in the case2 here, once the Q_EQ("col1", "2") is failed
    # during crawling of the filesystem, we ignore that branch. Thus always break your queries into smallest atomic
    # parts as possible, to save yourself `ls` calls.
    #
    # You can use the very same query mechanism when using the FixedColumnsParser -- the only difference is that there
    # you also have the filename column available.
    #
    # This approach works *only* if what you have a single list, or a cartesian product of lists. Eg, if you have
    # a year-month-day table, and you want the 1st and the 15th day of months in Q1, you can go with
    # `year=2022/month=[1,2,3]/day=[1,15]`. If, however, your query is a date range such as from the 14th June to
    # 17th September, you better head over to `test_daterange` which shows advanced capabilities.

    # You can now continue with either [date range utils](tests/test_daterange.py), or
    # [integrating with Dask](tests/test_dask.py). Furthermore, there is IdentityReader which provides a fancy `ls`
    # functionality. Lastly, you may want to inspect the `fsql/__init__.py` for information how to configure the S3
    # credentials.
