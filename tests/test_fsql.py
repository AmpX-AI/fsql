"""Central test collection for s3 utils.

Eventually, this will be broken into multiple test collections.
Mind the `conftest` to set up the mock/fake properly.
There is a `test_aaa_mock_works` method to increase the chance of a possible misconfiguration
being caught earlier.
"""

import logging
import warnings

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table, read_s3_table
from fsql.column_parser import AutoParser, FixedColumnsParser
from fsql.deser import EnumeratedDictReader, InputFormat, PandasReader
from fsql.query import Q_AND, Q_EQ, Q_OR, Q_TRUE, AtomicQuery

# TODO part of this is redundant due to test_example_usage -- clean up
# TODO some utils for this, to make the dataframe eval more intelligent
# TODO add parquet tests, and overall aim for a higher coverage
# TODO test performance? Ie, number of boto requests... we could introduce some perf tracker object...
# TODO the pathlib usage -- either spread it to all tests and api itself, or get rid of it

logging.basicConfig(level=logging.DEBUG, force=True)
logging.getLogger("botocore").setLevel(level=logging.WARNING)
logging.getLogger("s3fs").setLevel(level=logging.WARNING)
logging.getLogger("fsspec").setLevel(level=logging.WARNING)


def test_aaa_mock_works(helper):
    """To see that an s3 mock was properly set up, we first check that we *don't* see any real bucket,
    and then try to create a bucket, upload a file, download it again. The `aaa` naming is to run this
    test rather high in the automated order.
    """

    fs = helper.s3fs
    no_buckets_expected = fs.ls("/")
    assert len(no_buckets_expected) == 0
    with pytest.raises(FileNotFoundError):
        fs.ls("/data-lake-transformed")
    fs.mkdir("test-bouquet")
    helper.put_s3_file(b"hello", "/test-bouquet/testfile.txt")
    response = fs.ls("/test-bouquet")
    assert len(response) == 1
    assert response[0].split("/")[-1] == "testfile.txt"


def test_default_format(helper):
    """Creates data in a key=val-partitioned format, tests that partition filtering works correctly,
    and that files inside a partition are merged.
    """
    bucket = "test-bouquet"
    fs = helper.s3fs
    fs.mkdir(bucket)
    helper.put_s3_file(b'{"val": 1}', f"{bucket}/table1/partK1=1/partK2=1/read_me.json")
    helper.put_s3_file(b'{"val": 2}', f"{bucket}/table1/partK1=1/partK2=2/read_me_too.json")
    helper.put_s3_file(b'{"val": 3}', f"{bucket}/table1/partK1=1/partK2=2/me_read_as_well.json")
    helper.put_s3_file(b'{"val": 4}', f"{bucket}/table1/partK1=1/partK2=3/but_i_should_be_ignored.json")
    helper.put_s3_file(b'{"val": 5}', f"{bucket}/table1/partK1=2/partK2=1/the_same_here.json")
    helper.put_s3_file(b'{"val": 6}', f"{bucket}/table1/partK1=3/partK2=4/oh_and_this_read_too.json")

    def lt(partK2: str) -> bool:
        return int(partK2) <= 2

    query = Q_OR(Q_AND(Q_EQ("partK1", "1"), AtomicQuery(lt)), Q_EQ("partK1", "3"))
    reader = PandasReader(input_format=InputFormat.JSON)
    data = read_partitioned_table(f"s3://{bucket}/table1/", query, data_reader=reader)
    assert set(data.val.to_list()) == {1, 2, 3, 6}
    assert set(data.partK1.to_list()) == {"1", "3"}
    assert set(data.partK2.to_list()) == {"1", "2", "4"}

    # we also test the deprecated endpoint, at least to guarantee this basic coverage
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        data2 = read_s3_table(f"s3://{bucket}/table1/", query)
        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)

    assert_frame_equal(data, data2)


def test_fixed_column_format(helper):
    """Creates data in a val-partitioned format, tests that read with column names and subsequent filtering
    works correctly.
    """
    bucket = "test-bouquet"
    fs = helper.s3fs
    fs.mkdir(bucket)
    helper.put_s3_file(b'{"val": 1}', f"{bucket}/table2/read_me/something/read_me.json")
    helper.put_s3_file(b'{"val": 2}', f"{bucket}/table2/read_me/something_else/read_me_too.json")
    helper.put_s3_file(b'{"val": 3}', f"{bucket}/table2/ignore_me/dont_care/about_this.json")

    def f(first_column: str) -> bool:
        return first_column == "read_me"

    query = AtomicQuery(f)
    parser = FixedColumnsParser.from_str("first_column/second_column/fname")
    data = read_partitioned_table(f"s3://{bucket}/table2/", query, parser)
    assert set(data.val.to_list()) == {1, 2}
    assert set(data.first_column.to_list()) == {"read_me"}
    assert set(data.second_column.to_list()) == {"something", "something_else"}
    assert set(data.fname.to_list()) == {"read_me.json", "read_me_too.json"}


def test_partition_generation(helper):
    """Creates data in a val-partition format and constructs the parser to not query the fs for some."""
    bucket = "test-bouquet"
    fs = helper.s3fs
    fs.mkdir(bucket)
    helper.put_s3_file(b'{"val": 1}', f"{bucket}/table3/read_me/yes/i_will_be_there.json")
    helper.put_s3_file(b'{"val": 2}', f"{bucket}/table3/read_me/indeed/i_will_too.json")
    helper.put_s3_file(b'{"val": 3}', f"{bucket}/table3/read_me/no/i_will_not_be.json")
    helper.put_s3_file(b'{"val": 4}', f"{bucket}/table3/ignore_me/for_real/like_really.json")

    parser = FixedColumnsParser.from_str("first_column=read_me/second_column=[yes,indeed]/fname")
    data = read_partitioned_table(f"s3://{bucket}/table3/", Q_TRUE, parser)
    assert set(data.val.to_list()) == {1, 2}
    assert set(data.columns) == {"first_column", "second_column", "fname", "val"}

    parser = FixedColumnsParser.from_str("first_column=read_me/second_column=yes/fname=i_will_be_there.json")
    data = read_partitioned_table(f"s3://{bucket}/table3/", Q_TRUE, parser)
    assert set(data.val.to_list()) == {1}


def test_read_local(tmp_path):
    """Creates data in a key=val-partition format, constructs the parser to not query the fs for some; in local FS."""
    parser = AutoParser.from_str("first_column=[one,two]/second_column")
    part1 = tmp_path / "first_column=one" / "second_column=x"
    part1.mkdir(parents=True)
    (part1 / "f.json").write_text('{"val": 1}')
    part2 = tmp_path / "first_column=two" / "second_column=x"
    part2.mkdir(parents=True)
    (part2 / "f.json").write_text('{"val": 2}')
    part3 = tmp_path / "first_column=three" / "second_column=x"
    part3.mkdir(parents=True)
    (part3 / "f.json").write_text('{"val": 3}')
    print(f"{tmp_path}")
    data = read_partitioned_table(f"file://{tmp_path}/", Q_TRUE, parser)
    print(data)
    assert set(data.val.to_list()) == {1, 2}
    assert set(data.first_column.to_list()) == {"one", "two"}
    assert set(data.second_column.to_list()) == {"x"}
    assert set(data.columns) == {"first_column", "second_column", "val"}


def test_duplicate_partition_error(tmp_path):
    p1 = tmp_path / "c1=4" / "c1=3"
    p1.mkdir(parents=True)
    df = pd.DataFrame({"a": [0, 1]})
    df.to_parquet(p1 / "f1.parquet")
    with pytest.raises(ValueError, match="duplicate key inserted: c1"):
        read_partitioned_table(f"file://{tmp_path}/", Q_TRUE)


def test_not_enough_partitions(tmp_path):
    p1 = tmp_path / "v1" / "v2"
    p1.mkdir(parents=True)
    df = pd.DataFrame({"a": [0, 1]})
    df.to_parquet(p1 / "f1.parquet")
    parser = FixedColumnsParser.from_str("c1/fname")
    with pytest.raises(ValueError, match="no partitions remaining"):
        read_partitioned_table(f"file://{tmp_path}/", Q_TRUE, parser)


def test_broken_format_autodetect(tmp_path):
    """This test is kinda weird -- we artifically break format detection capabilities."""
    reader = PandasReader()
    reader.detect_format = lambda _: InputFormat.AUTO
    p1 = tmp_path / "c1=v1"
    p1.mkdir(parents=True)
    df = pd.DataFrame({"a": [0, 1]})
    df.to_parquet(p1 / "f1.parquet")
    with pytest.raises(ValueError, match="partition had format detected as auto -> invalid state"):
        read_partitioned_table(f"file://{tmp_path}/", Q_TRUE, data_reader=reader)


def test_dict_reader_nonjson(tmp_path):
    p1 = tmp_path / "c1=v1"
    p1.mkdir(parents=True)
    df = pd.DataFrame({"a": [0, 1]})
    df.to_parquet(p1 / "f1.parquet")
    reader = EnumeratedDictReader()
    with pytest.raises(ValueError, match="EnumeratedDictReader supports only json"):
        read_partitioned_table(f"file://{tmp_path}/", Q_TRUE, data_reader=reader)
