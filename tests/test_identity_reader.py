import pandas as pd
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.column_parser import AutoParser
from fsql.deser import IDENTITY_READER
from fsql.query import Q_TRUE

df1 = pd.DataFrame(data={"c1": [0, 1], "c2": ["hello", "world"]})
df2 = pd.DataFrame(data={"c1": [2, 3], "c2": ["salve", "mundi"]})
df3 = pd.DataFrame(data={"c1": [4, 5], "c2": ["cthulhu", "rlyeh"]})


def test_identity_reader(tmp_path):
    case1_path = tmp_path / "table1"
    case1_path.mkdir(parents=True)
    df1.to_csv(case1_path / "f1.csv", index=False)

    case1_result_r = read_partitioned_table(f"file://{case1_path}/", Q_TRUE, data_reader=IDENTITY_READER)
    case1_result = list(case1_result_r)

    assert len(case1_result) == 1
    assert case1_result[0].file_url == f"/{case1_path}/f1.csv"
    assert case1_result[0].partition_values == {}

    case1_deserd = case1_result[0].consume(pd.read_csv)
    assert_frame_equal(df1, case1_deserd)

    case2_path = tmp_path / "table2"
    case2_part1 = case2_path / "c3=42" / "c4=test"
    case2_part1.mkdir(parents=True)
    case2_part2 = case2_path / "c3=43" / "c4=test"
    case2_part2.mkdir(parents=True)
    case2_part3 = case2_path / "c3=44" / "c4=test"
    case2_part3.mkdir(parents=True)
    df1.to_csv(case2_part1 / "f1.csv", index=False)
    df2.to_csv(case2_part2 / "f2.csv", index=False)
    df3.to_csv(case2_part3 / "f3.csv", index=False)

    parser = AutoParser.from_str("c3=[42,43]/c4=[test]")
    case2_result_r = read_partitioned_table(
        f"file://{case2_path}/", Q_TRUE, column_parser=parser, data_reader=IDENTITY_READER
    )
    case2_result = list(case2_result_r)

    assert len(case2_result) == 2
    assert case2_result[0].file_url == f"/{case2_path}/c3=42/c4=test/f1.csv"
    assert case2_result[1].file_url == f"/{case2_path}/c3=43/c4=test/f2.csv"
    assert case2_result[0].partition_values == {"c3": "42", "c4": "test"}
    assert case2_result[1].partition_values == {"c3": "43", "c4": "test"}

    case2_deserd1 = case2_result[0].consume(pd.read_csv)
    assert_frame_equal(df1, case2_deserd1)
    case2_deserd2 = case2_result[1].consume(pd.read_csv)
    assert_frame_equal(df2, case2_deserd2)
